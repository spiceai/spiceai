use bollard::secret::HealthConfig;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use secrecy::SecretString;
use std::collections::HashMap;
use tracing::instrument;

use crate::{
    container_registry,
    docker::{ContainerRunnerBuilder, RunningContainer},
};

const MYSQL_ROOT_PASSWORD: &str = "integration-test-pw";
const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-mysql";

pub(super) fn get_mysql_params(
    port: usize,
    time_zone: Option<&str>,
) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "mysql_host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert(
        "mysql_tcp_port".to_string(),
        SecretString::from(port.to_string()),
    );
    params.insert(
        "mysql_user".to_string(),
        SecretString::from("root".to_string()),
    );
    params.insert(
        "mysql_pass".to_string(),
        SecretString::from(MYSQL_ROOT_PASSWORD.to_string()),
    );
    params.insert(
        "mysql_db".to_string(),
        SecretString::from("mysqldb".to_string()),
    );
    params.insert(
        "mysql_sslmode".to_string(),
        SecretString::from("disabled".to_string()),
    );
    if let Some(tz) = time_zone {
        params.insert(
            "mysql_connection_time_zone".to_string(),
            SecretString::from(tz.to_string()),
        );
    }
    params.insert(
        "mysql_pool_min".to_string(),
        SecretString::from("1".to_string()),
    );
    params.insert(
        "mysql_pool_max".to_string(),
        SecretString::from("10".to_string()),
    );
    params
}

#[instrument]
pub async fn start_mysql_docker_container(port: usize) -> Result<RunningContainer, anyhow::Error> {
    let container_name = format!("{MYSQL_DOCKER_CONTAINER}-{port}");

    let port = port.try_into().unwrap_or(15432);

    let mysql_docker_image = std::env::var("MYSQL_DOCKER_IMAGE")
        .unwrap_or_else(|_| format!("{}mysql:latest", container_registry()));

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(mysql_docker_image)
        .add_port_binding(3306, port)
        .add_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .add_env_var("MYSQL_DATABASE", "mysqldb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "mysqladmin ping --host=127.0.0.1 --port=3306 --password={MYSQL_ROOT_PASSWORD}"
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
pub(super) async fn get_mysql_connection_pool(
    port: usize,
    time_zone: Option<&str>,
) -> Result<MySQLConnectionPool, anyhow::Error> {
    let mysql_pool = MySQLConnectionPool::new(get_mysql_params(port, time_zone))
        .await
        .expect("Failed to create MySQL Connection Pool");

    Ok(mysql_pool)
}
