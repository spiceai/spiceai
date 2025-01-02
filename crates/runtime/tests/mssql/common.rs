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
use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

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
                    "/opt/mssql-tools/bin/sqlcmd -C -U sa -P {MSSQL_ROOT_PASSWORD} -Q \"SELECT 1\""
                ),
            ]),
            interval: Some(250_000_000),
            timeout: Some(100_000_000),
            retries: Some(5),
            start_period: Some(500_000_000),
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}
