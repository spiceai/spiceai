/*
Copyright 2026 The Spice.ai OSS Authors

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

use crate::docker::ContainerRunnerBuilder;
use crate::docker::RunningContainer;

const ES_DOCKER_CONTAINER: &str = "runtime-integration-test-elasticsearch";
const ES_IMAGE: &str = "docker.elastic.co/elasticsearch/elasticsearch:8.17.0";

/// Starts a single-node Elasticsearch container with security disabled.
///
/// Returns the [`RunningContainer`] handle — drop or call `.remove()` to clean up.
pub(super) async fn start_elasticsearch_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{ES_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());

    // ES images are only available from the official Elastic registry, not the
    // project-wide CONTAINER_REGISTRY mirror, so we always use the canonical image.

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(ES_IMAGE.to_string())
        .add_port_binding(9200, port)
        .add_env_var("discovery.type", "single-node")
        .add_env_var("xpack.security.enabled", "false")
        .add_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "curl -sf http://localhost:9200/_cluster/health || exit 1".to_string(),
            ]),
            interval: Some(2_000_000_000), // 2s
            timeout: Some(10_000_000_000), // 10s
            retries: Some(30),
            start_period: Some(15_000_000_000), // 15s
            start_interval: None,
        })
        .build()?
        .run(Some(std::time::Duration::from_mins(2)))
        .await?;

    Ok(running_container)
}

/// Returns the Elasticsearch endpoint URL for a container started on `port`.
pub(super) fn elasticsearch_endpoint(port: u16) -> String {
    format!("http://localhost:{port}")
}
