use bollard::secret::HealthConfig;
use datafusion_table_providers::{
    sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool, util::secrets::to_secret_map,
};
use secrecy::SecretString;
use std::collections::HashMap;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const CLICKHOUSE_USER: &str = "user";
const CLICKHOUSE_PASSWORD: &str = "integration-test-pw";
const CLICKHOUSE_DOCKER_CONTAINER: &str = "runtime-integration-test-clickhouse";

pub(super) fn get_clickhouse_params() -> HashMap<String, SecretString> {
    to_secret_map(HashMap::from([
        ("url".to_string(), "http://localhost:8123".to_string()),
        ("user".to_string(), CLICKHOUSE_USER.to_string()),
        ("password".to_string(), CLICKHOUSE_PASSWORD.to_string()),
    ]))
}

#[instrument]
pub async fn start_clickhouse_docker_container() -> Result<RunningContainer, anyhow::Error> {
    let container_name = CLICKHOUSE_DOCKER_CONTAINER;

    let clickhouse_docker_image = std::env::var("CLICKHOUSE_DOCKER_IMAGE")
        .unwrap_or_else(|_| format!("{}clickhouse:latest", "registry.hub.docker.com/library/"));

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(clickhouse_docker_image)
        .add_port_binding(8123, 8123)
        .add_env_var("CLICKHOUSE_USER", CLICKHOUSE_USER)
        .add_env_var("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "wget --no-verbose --tries=1 --spider http://localhost:8123/ping || exit 1"
                ),
            ]),
            interval: Some(500_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(5),
            start_period: Some(500_000_000), // 100ms
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    Ok(running_container)
}

#[instrument]
#[allow(dead_code)]
pub(super) async fn get_mysql_connection_pool(
    port: usize,
) -> Result<ClickHouseConnectionPool, anyhow::Error> {
    let pool = ClickHouseConnectionPool::new(get_clickhouse_params())
        .await
        .expect("Failed to create MySQL Connection Pool");
    Ok(pool)
}
