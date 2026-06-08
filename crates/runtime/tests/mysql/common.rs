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
use mysql_async::prelude::Queryable;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub fn make_mysql_dataset(path: &str, name: &str, port: u16, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("mysql:{path}"), name.to_string());
    let params = HashMap::from([
        ("mysql_host".to_string(), "localhost".to_string()),
        ("mysql_tcp_port".to_string(), port.to_string()),
        ("mysql_user".to_string(), "root".to_string()),
        ("mysql_pass".to_string(), "integration-test-pw".to_string()),
        ("mysql_db".to_string(), "mysqldb".to_string()),
        ("mysql_sslmode".to_string(), "disabled".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

const MYSQL_ROOT_PASSWORD: &str = "integration-test-pw";
const MYSQL_IMAGE: &str = "docker.io/library/mysql:latest";
const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-mysql";
const MYSQL_CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(3);
const MYSQL_HOST_PORT_READY_TIMEOUT: Duration = Duration::from_mins(1);

#[instrument]
pub async fn start_mysql_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{MYSQL_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(MYSQL_IMAGE.to_string())
        .add_port_binding(3306, port)
        .add_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .add_env_var("MYSQL_DATABASE", "mysqldb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "mysqladmin ping -h127.0.0.1 -uroot --password={MYSQL_ROOT_PASSWORD} --silent"
                ),
            ]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(120),
            start_period: Some(30_000_000_000), // 30s
            start_interval: None,
        })
        .build()?
        .run(Some(MYSQL_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_mysql_host_port(port).await?;

    Ok(running_container)
}

async fn wait_for_mysql_host_port(port: u16) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let mut last_error = None;
    let pool = get_mysql_conn(port)?;

    while start_time.elapsed() <= MYSQL_HOST_PORT_READY_TIMEOUT {
        match pool.get_conn().await {
            Ok(mut conn) => match conn.query_drop("SELECT 1").await {
                Ok(()) => {
                    drop(conn);
                    pool.disconnect().await?;
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                }
            },
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let last_error = last_error.unwrap_or_else(|| "none".to_string());
    pool.disconnect().await.map_err(|error| {
        anyhow::anyhow!(
            "MySQL container started but host port {port} was not ready within {}s. Last error: {last_error}; failed to disconnect readiness pool: {error}",
            MYSQL_HOST_PORT_READY_TIMEOUT.as_secs()
        )
    })?;

    Err(anyhow::anyhow!(
        "MySQL container started but host port {port} was not ready within {}s. Last error: {}",
        MYSQL_HOST_PORT_READY_TIMEOUT.as_secs(),
        last_error
    ))
}

#[instrument]
pub fn get_mysql_conn(port: u16) -> Result<mysql_async::Pool, anyhow::Error> {
    let url = format!("mysql://root:{MYSQL_ROOT_PASSWORD}@localhost:{port}/mysqldb",);
    let opts_builder =
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(url.as_str())?);
    let opts = mysql_async::Opts::from(opts_builder);

    Ok(mysql_async::Pool::new(opts))
}
