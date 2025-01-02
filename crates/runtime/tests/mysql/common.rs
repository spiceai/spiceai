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

use std::collections::HashMap;

use bollard::secret::HealthConfig;
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params as DatasetParams,
};
use tracing::instrument;

use crate::{
    container_registry,
    docker::{ContainerRunnerBuilder, RunningContainer},
};

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

#[instrument]
pub async fn start_mysql_docker_container(
    container_name: &'static str,
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(format!("{}mysql:latest", container_registry()))
        .add_port_binding(3306, port)
        .add_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .add_env_var("MYSQL_DATABASE", "mysqldb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!("mysqladmin ping --password={MYSQL_ROOT_PASSWORD}"),
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
pub fn get_mysql_conn(port: u16) -> Result<mysql_async::Pool, anyhow::Error> {
    let url = format!("mysql://root:{MYSQL_ROOT_PASSWORD}@localhost:{port}/mysqldb",);
    let opts_builder =
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(url.as_str())?);
    let opts = mysql_async::Opts::from(opts_builder);

    Ok(mysql_async::Pool::new(opts))
}
