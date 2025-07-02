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

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub const ORACLE_USERNAME: &str = "system";
pub const ORACLE_ROOT_PASSWORD: &str = "S3cret_Pass123";

pub fn make_oracle_dataset(path: &str, name: &str, port: u16) -> Dataset {
    let mut dataset = Dataset::new(format!("oracle:{path}"), name.to_string());
    let params = HashMap::from([
        ("oracle_host".to_string(), "localhost".to_string()),
        ("oracle_port".to_string(), format!("{port}")),
        ("oracle_username".to_string(), ORACLE_USERNAME.to_string()),
        (
            "oracle_password".to_string(),
            ORACLE_ROOT_PASSWORD.to_string(),
        ),
        ("oracle_service_name".to_string(), "FREEPDB1".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

#[instrument]
pub async fn start_oracle_docker_container(
    container_name: &'static str,
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image("gvenzl/oracle-free:latest".to_string())
        .add_port_binding(1521, port)
        .add_env_var("ORACLE_PASSWORD", ORACLE_ROOT_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec!["CMD-SHELL".to_string(), "healthcheck.sh".to_string()]),
            interval: Some(10_000_000_000), // 10 seconds between checks
            timeout: Some(5_000_000_000),   // 5 seconds max wait per check
            retries: Some(10),              // 10 retries
            start_period: None,
            start_interval: None,
        })
        .build()?
        // Average time to start container is 60s, we set longer timeout to ensure enough time for the container to start
        .run(Some(Duration::from_secs(120)))
        .await?;

    Ok(running_container)
}
