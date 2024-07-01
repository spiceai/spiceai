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

use bollard::secret::HealthConfig;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

#[instrument]
pub async fn start_redpanda_docker_container() -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container: RunningContainer = ContainerRunnerBuilder::new(
        "redpanda-spice-integration",
    )
    .image("docker.redpanda.com/redpandadata/redpanda:v24.1.8") // Same as the normal Postgres image with logical decoding enabled, which is needed for Debezium
    .add_port_binding(18081, 18081)
    .add_port_binding(18082, 18082)
    .add_port_binding(19092, 19092)
    .add_port_binding(9644, 9644)
    .add_cmd("redpanda")
    .add_cmd("start")
    .add_cmd("--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092")
    .add_cmd("--advertise-kafka-addr internal://redpanda-kafka-0:9092,external://localhost:19092")
    .add_cmd("--pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082")
    .add_cmd(
        "--advertise-pandaproxy-addr internal://redpanda-kafka-0:8082,external://localhost:18082",
    )
    .add_cmd("--schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081")
    .add_cmd("--rpc-addr redpanda-kafka-0:33145")
    .add_cmd("--advertise-rpc-addr redpanda-kafka-0:33145")
    .add_cmd("--mode dev-container")
    .add_cmd("--smp 1")
    .add_cmd("--default-log-level=info")
    .healthcheck(HealthConfig {
        test: Some(vec![
            "CMD-SHELL".to_string(),
            "rpk cluster health | grep -E 'Healthy:.+true' || exit 1".to_string(),
        ]),
        interval: Some(250_000_000),  // 250ms
        timeout: Some(3_000_000_000), // 3s
        retries: Some(5),
        start_period: Some(5_000_000_000), // 5s
        start_interval: None,
    })
    .build()?
    .run()
    .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}
