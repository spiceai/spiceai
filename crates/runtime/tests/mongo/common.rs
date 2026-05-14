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
#[cfg(feature = "duckdb")]
use spicepod::acceleration::{OnConflictBehavior, RefreshMode};
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const MONGODB_ROOT_PASSWORD: &str = "integration-test-pw";
const MONGODB_IMAGE: &str = "docker.io/library/mongo:latest";
const MONGODB_DOCKER_CONTAINER: &str = "runtime-integration-test-mongo";
const MONGODB_CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(180);
const MONGODB_HOST_PORT_READY_TIMEOUT: Duration = Duration::from_secs(60);

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

#[cfg(feature = "duckdb")]
pub fn make_mongodb_change_stream_dataset(path: &str, name: &str, port: u16) -> Dataset {
    let mut dataset = Dataset::new(format!("mongodb:{path}"), name.to_string());
    let connection_string = format!(
        "mongodb://root:{MONGODB_ROOT_PASSWORD}@localhost:{port}/testdb?authSource=admin&directConnection=true&replicaSet=rs0"
    );
    let params = HashMap::from([
        ("mongodb_connection_string".to_string(), connection_string),
        (
            "change_stream_batch_max_duration".to_string(),
            "100ms".to_string(),
        ),
        (
            "change_stream_max_await_time".to_string(),
            "100ms".to_string(),
        ),
        ("change_stream_batch_size".to_string(), "10".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some("_id".to_string()),
        on_conflict: HashMap::from([("_id".to_string(), OnConflictBehavior::Upsert)]),
        ..Default::default()
    });
    dataset
}

#[instrument]
pub async fn start_mongodb_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{MONGODB_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(MONGODB_IMAGE.to_string())
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
            interval: Some(2_000_000_000), // 2 seconds
            timeout: Some(10_000_000_000), // 10 seconds
            retries: Some(15),
            start_period: Some(10_000_000_000), // 10 seconds
            start_interval: None,
        })
        .build()?
        .run(Some(MONGODB_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_mongodb_host_port(port).await?;
    Ok(running_container)
}

#[cfg(feature = "duckdb")]
#[instrument]
pub async fn start_mongodb_replica_set_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{MONGODB_DOCKER_CONTAINER}-rs-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(MONGODB_IMAGE.to_string())
        .add_port_binding(27017, port)
        .add_env_var("MONGO_INITDB_ROOT_USERNAME", "root")
        .add_env_var("MONGO_INITDB_ROOT_PASSWORD", MONGODB_ROOT_PASSWORD)
        .add_env_var("MONGO_INITDB_DATABASE", "testdb")
        .command(["mongod", "--replSet", "rs0", "--bind_ip_all"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD".to_string(),
                "mongosh".to_string(),
                "--quiet".to_string(),
                "--eval".to_string(),
                "db.runCommand('ping').ok".to_string(),
            ]),
            interval: Some(2_000_000_000),
            timeout: Some(10_000_000_000),
            retries: Some(15),
            start_period: Some(10_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(MONGODB_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_mongodb_host_port(port).await?;
    initiate_mongodb_replica_set(&running_container).await?;
    Ok(running_container)
}

#[cfg(feature = "duckdb")]
async fn initiate_mongodb_replica_set(
    running_container: &RunningContainer<'_>,
) -> Result<(), anyhow::Error> {
    let initiate = format!(
        "mongosh --quiet -u root -p {MONGODB_ROOT_PASSWORD} --authenticationDatabase admin --eval rs.initiate({{_id:'rs0',members:[{{_id:0,host:'localhost:27017'}}]}})"
    );
    let _ = running_container.exec_cmd(&initiate).await?;

    let start_time = std::time::Instant::now();
    let mut last_output = None;
    let status_command = format!(
        "mongosh --quiet -u root -p {MONGODB_ROOT_PASSWORD} --authenticationDatabase admin --eval rs.status().myState"
    );

    while start_time.elapsed() <= MONGODB_HOST_PORT_READY_TIMEOUT {
        match running_container.exec_cmd(&status_command).await {
            Ok(output) if output.trim() == "1" => return Ok(()),
            Ok(output) => last_output = Some(output),
            Err(error) => last_output = Some(error.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!(
        "MongoDB replica set did not become primary within {}s. Last output: {}",
        MONGODB_HOST_PORT_READY_TIMEOUT.as_secs(),
        last_output.unwrap_or_else(|| "none".to_string())
    ))
}

async fn wait_for_mongodb_host_port(port: u16) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let mut last_error = None;

    while start_time.elapsed() <= MONGODB_HOST_PORT_READY_TIMEOUT {
        match get_mongodb_client(port).await {
            Ok(client) => match client
                .database("admin")
                .run_command(mongodb::bson::doc! { "ping": 1 })
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) => last_error = Some(error.to_string()),
            },
            Err(error) => last_error = Some(error.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!(
        "MongoDB container started but host port {port} was not ready within {}s. Last error: {}",
        MONGODB_HOST_PORT_READY_TIMEOUT.as_secs(),
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

#[instrument]
pub async fn get_mongodb_client(port: u16) -> Result<mongodb::Client, anyhow::Error> {
    let uri =
        format!("mongodb://root:{MONGODB_ROOT_PASSWORD}@localhost:{port}/testdb?authSource=admin");
    tracing::debug!("Connecting to MongoDB at localhost:{port}/testdb");
    let client = mongodb::Client::with_uri_str(&uri).await?;
    Ok(client)
}

#[cfg(feature = "duckdb")]
#[instrument]
pub async fn get_mongodb_replica_set_client(port: u16) -> Result<mongodb::Client, anyhow::Error> {
    let uri = format!(
        "mongodb://root:{MONGODB_ROOT_PASSWORD}@localhost:{port}/testdb?authSource=admin&directConnection=true&replicaSet=rs0"
    );
    tracing::debug!("Connecting to MongoDB replica set at localhost:{port}/testdb");
    let client = mongodb::Client::with_uri_str(&uri).await?;
    Ok(client)
}
