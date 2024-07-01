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

use std::time::Duration;

use bollard::secret::HealthConfig;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub async fn start_redpanda_debezium_docker_containers(
) -> (RunningContainer<'static>, RunningContainer<'static>) {
    let redpanda_container = start_redpanda_docker_container().await;
    let debezium_container = start_debezium_docker_container().await;

    (redpanda_container, debezium_container)
}

#[instrument]
async fn start_redpanda_docker_container() -> RunningContainer<'static> {
    let running_container: RunningContainer =
        ContainerRunnerBuilder::new("redpanda-spice-integration")
            .image("docker.redpanda.com/redpandadata/redpanda:v24.1.8")
            .add_port_binding(19092, 19092)
            //.add_port_binding(9644, 9644)
            .add_cmd("redpanda")
            .add_cmd("start")
            .add_cmd("--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092")
            .add_cmd("--advertise-kafka-addr internal://localhost:9092,external://172.17.0.1:19092")
            // .add_cmd("--pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082")
            // .add_cmd(
            //     "--advertise-pandaproxy-addr internal://localhost:8082,external://172.17.0.1:18082",
            // )
            .add_cmd("--schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081")
            .add_cmd("--rpc-addr localhost:33145")
            .add_cmd("--advertise-rpc-addr localhost:33145")
            .add_cmd("--mode dev-container")
            .add_cmd("--smp 1")
            .add_cmd("--default-log-level=info")
            .healthcheck(HealthConfig {
                test: Some(vec![
                    "CMD".to_string(),
                    "nc".to_string(),
                    "-z".to_string(),
                    "localhost".to_string(),
                    "9092".to_string(),
                ]),
                interval: Some(250_000_000),  // 250ms
                timeout: Some(3_000_000_000), // 3s
                retries: Some(5),
                start_period: Some(5_000_000_000), // 5s
                start_interval: None,
            })
            .build()
            .expect("container to build")
            .run()
            .await
            .expect("container to run");

    running_container
}

/// Needs to run after `start_redpanda_docker_container`
#[instrument]
async fn start_debezium_docker_container() -> RunningContainer<'static> {
    let running_container: RunningContainer =
        ContainerRunnerBuilder::new("debezium-spice-integration")
            .image("debezium/connect:2.7")
            .add_port_binding(8083, 8083)
            .add_env_var("BOOTSTRAP_SERVERS", "172.17.0.1:19092")
            .add_env_var("GROUP_ID", "1")
            .add_env_var("CONFIG_STORAGE_TOPIC", "connect_configs")
            .add_env_var("OFFSET_STORAGE_TOPIC", "connect_offsets")
            .build()
            .expect("container to build")
            .run()
            .await
            .expect("container to run");

    tokio::time::sleep(Duration::from_secs(5)).await;

    running_container
}

pub async fn register_postgres_connector() -> Result<(), anyhow::Error> {
    let response = reqwest::Client::new()
        .post("http://localhost:8083/connectors")
        .json(&serde_json::json!({
            "name": "postgres-connector",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "172.17.0.1",
                "plugin.name":"pgoutput",
                "tasks.max": "1",
                "database.port": "5432",
                "database.user": "postgres",
                "database.password": "postgres",
                "database.dbname": "postgres",
                "schema.include.list":"public",
                "database.server.name":"postgres-server",
                "topic.prefix": "cdc"
            }
        }))
        .send()
        .await?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Failed to register postgres connector"))
    }
}
