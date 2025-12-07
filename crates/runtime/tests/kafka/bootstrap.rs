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
use std::time::Duration;

use bollard::secret::HealthConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::{config::ClientConfig, producer::FutureRecord};
use spicepod::acceleration::{Acceleration, RefreshMode};
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub const KAFKA_DOCKER_CONTAINER: &str = "runtime-integration-test-kafka";
pub const KAFKA_SASL_USERNAME: &str = "kafka";
pub const KAFKA_SASL_PASSWORD: &str = "kafka123";
pub const KAFKA_SASL_MECHANISM: &str = "SCRAM-SHA-256";

#[instrument]
pub async fn start_kafka_docker_container(
    port: u16,
    topics: &[&str],
) -> Result<(RunningContainer<'static>, FutureProducer), anyhow::Error> {
    let container_name = format!("{KAFKA_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        // Use Redpanda (Kafka-API compatible) as for dev/test purpose:
        // single binary (no JVM), fast startup, smaller CPU/RAM footprint
        // than apache/kafka - ideal for CI and local tests.
        .image("redpandadata/redpanda:latest".to_string())
        .command([
            "redpanda",
            "start",
            "--set",
            "redpanda.enable_sasl=true",
            "--set",
            &format!(r#"redpanda.superusers=["{KAFKA_SASL_USERNAME}"]"#),
            "--smp",
            "1",
            "--overprovisioned",
            "--node-id",
            "0",
            "--mode",
            "dev-container",
            &format!("--kafka-addr=SASL_PLAINTEXT://0.0.0.0:{port}"),
            &format!("--advertise-kafka-addr=SASL_PLAINTEXT://127.0.0.1:{port}"),
        ])
        .add_port_binding(port, port)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "rpk cluster health | grep -E 'Healthy:.+true' || exit 1".to_string(),
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(10),
            start_period: Some(500_000_000), // 500ms
            start_interval: None,
        })
        .build()?
        .run(None)
        .await?;

    tracing::debug!("Kafka user creation command result: {}", running_container.exec_cmd(
        &format!("rpk acl user create {KAFKA_SASL_USERNAME} -p {KAFKA_SASL_PASSWORD} --mechanism {KAFKA_SASL_MECHANISM} -X brokers=localhost:{port}"),
    )
    .await?);

    for topic in topics {
        tracing::debug!(
            "Kafka topic '{topic}' creation command result: {}",
            running_container
                .exec_cmd(&format!(
                    "rpk topic create {topic} \
                --brokers localhost:{port} \
                --user {KAFKA_SASL_USERNAME} \
                --password {KAFKA_SASL_PASSWORD} \
                --sasl-mechanism {KAFKA_SASL_MECHANISM}"
                ),)
                .await?
        );
    }

    Ok((
        running_container,
        create_kafka_producer(
            &format!("localhost:{port}"),
            Some(KAFKA_SASL_USERNAME),
            Some(KAFKA_SASL_PASSWORD),
        )?,
    ))
}

pub fn create_kafka_producer(
    broker: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<FutureProducer, anyhow::Error> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");

    if let (Some(user), Some(pass)) = (username, password) {
        config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", KAFKA_SASL_MECHANISM)
            .set("sasl.username", user)
            .set("sasl.password", pass);
    } else {
        config.set("security.protocol", "PLAINTEXT");
    }

    let producer: FutureProducer = config.create()?;
    Ok(producer)
}

pub async fn send_messages_to_kafka<T>(
    producer: &FutureProducer,
    topic: &str,
    messages: &[T],
) -> Result<(), anyhow::Error>
where
    T: serde::Serialize,
{
    const MAX_RETRIES: u32 = 5;
    const DELAY_S: u64 = 1;
    const QUEUE_TIMEOUT: Duration = Duration::from_secs(2);

    for message in messages {
        let message_str = serde_json::to_string(message)?;

        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            let record = FutureRecord::<String, String>::to(topic).payload(&message_str);

            match producer.send(record, QUEUE_TIMEOUT).await {
                Ok(_) => {
                    if attempt > 0 {
                        tracing::debug!("Message sent successfully after {attempt} retries");
                    }
                    last_error = None;
                    break;
                }
                Err((e, _)) if attempt < MAX_RETRIES => {
                    tracing::debug!(
                        "Kafka send failed (attempt {}/{}): {e}. Retrying in {DELAY_S} seconds",
                        attempt + 1,
                        MAX_RETRIES + 1,
                    );
                    last_error = Some(e);
                    tokio::time::sleep(Duration::from_secs(DELAY_S)).await;
                }
                Err((e, _)) => {
                    last_error = Some(e);
                }
            }
        }

        if let Some(e) = last_error {
            return Err(anyhow::Error::msg(format!(
                "Kafka message delivery failed after {} attempts: {e}",
                MAX_RETRIES + 1,
            )));
        }
    }
    Ok(())
}

pub fn make_kafka_dataset(
    path: &str,
    name: &str,
    port: u16,
    extra_params: Option<HashMap<String, String>>,
) -> Dataset {
    let mut params = HashMap::from([
        (
            "kafka_bootstrap_servers".to_string(),
            format!("localhost:{port}"),
        ),
        (
            "kafka_security_protocol".to_string(),
            "SASL_PLAINTEXT".to_string(),
        ),
        (
            "kafka_sasl_mechanism".to_string(),
            KAFKA_SASL_MECHANISM.to_string(),
        ),
        (
            "kafka_sasl_username".to_string(),
            KAFKA_SASL_USERNAME.to_string(),
        ),
        (
            "kafka_sasl_password".to_string(),
            KAFKA_SASL_PASSWORD.to_string(),
        ),
    ]);

    if let Some(extra) = extra_params {
        params.extend(extra);
    }

    let mut dataset = Dataset::new(format!("kafka:{path}"), name.to_string());
    dataset.params = Some(DatasetParams::from_string_map(params));

    // Kafka connector requires Append mode acceleration
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_mode: Some(RefreshMode::Append),
        ..Default::default()
    });

    dataset
}
