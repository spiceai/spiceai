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
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port};

pub fn make_mssql_dataset(path: &str, name: &str, port: u16) -> Dataset {
    let mut dataset = Dataset::new(format!("mssql:{path}"), name.to_string());
    let params = HashMap::from([
        ("mssql_host".to_string(), "localhost".to_string()),
        ("mssql_port".to_string(), port.to_string()),
        ("mssql_username".to_string(), "sa".to_string()),
        (
            "mssql_password".to_string(),
            MSSQL_ROOT_PASSWORD.to_string(),
        ),
        ("mssql_encrypt".to_string(), "false".to_string()),
        (
            "mssql_trust_server_certificate".to_string(),
            "true".to_string(),
        ),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

pub const MSSQL_ROOT_PASSWORD: &str = "S3cret-integration-test-p@ss";
const MSSQL_CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(3);
const MSSQL_HOST_PORT_READY_TIMEOUT: Duration = Duration::from_mins(1);

#[instrument]
pub async fn start_mssql_docker_container(
    container_name: &'static str,
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image("mcr.microsoft.com/mssql/server:2022-latest".to_string())
        .add_port_binding(1433, port)
        .add_env_var("MSSQL_SA_PASSWORD", MSSQL_ROOT_PASSWORD)
        .add_env_var("ACCEPT_EULA", "Y")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "/opt/mssql-tools18/bin/sqlcmd -C -U sa -P {MSSQL_ROOT_PASSWORD} -Q \"SELECT 1\""
                ),
            ]),
            interval: Some(1_000_000_000),
            timeout: Some(5_000_000_000),
            retries: Some(120),
            start_period: Some(30_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(MSSQL_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_tcp_port("127.0.0.1", port, MSSQL_HOST_PORT_READY_TIMEOUT).await?;
    Ok(running_container)
}
