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
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const MONGODB_ROOT_PASSWORD: &str = "integration-test-pw";
const MONGODB_DOCKER_CONTAINER: &str = "runtime-integration-test-mongo";

pub fn make_mongodb_dataset(path: &str, name: &str, port: u16, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("mongodb:{path}"), name.to_string());
    let params = HashMap::from([
        ("mongodb_host".to_string(), "localhost".to_string()),
        ("mongodb_port".to_string(), port.to_string()),
        ("mongodb_user".to_string(), "root".to_string()),
        (
            "mongodb_pass".to_string(),
            MONGODB_ROOT_PASSWORD.to_string(),
        ),
        ("mongodb_db".to_string(), "testdb".to_string()),
        ("mongodb_auth_source".to_string(), "admin".to_string()),
        ("mongodb_sslmode".to_string(), "disabled".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

#[instrument]
pub async fn start_mongodb_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{MONGODB_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image("mongo:latest".to_string())
        .add_port_binding(27017, port)
        .add_env_var("MONGO_INITDB_ROOT_USERNAME", "root")
        .add_env_var("MONGO_INITDB_ROOT_PASSWORD", MONGODB_ROOT_PASSWORD)
        .add_env_var("MONGO_INITDB_DATABASE", "testdb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD".to_string(),
                "mongosh".to_string(),
                "--quiet".to_string(),
                "--eval".to_string(),
                "db.runCommand('ping').ok".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(500_000_000),
            retries: Some(10),
            start_period: Some(3_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(None)
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

#[instrument]
pub async fn get_mongodb_client(port: u16) -> Result<mongodb::Client, anyhow::Error> {
    let uri =
        format!("mongodb://root:{MONGODB_ROOT_PASSWORD}@localhost:{port}/testdb?authSource=admin");
    tracing::debug!("Connecting to MongoDB at {}", uri);
    let client = mongodb::Client::with_uri_str(&uri).await?;
    Ok(client)
}
