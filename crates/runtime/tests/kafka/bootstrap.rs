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
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::{config::ClientConfig, producer::FutureRecord};
use spicepod::acceleration::{Acceleration, RefreshMode};
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub const KAFKA_DOCKER_CONTAINER: &str = "runtime-integration-test-kafka";
pub const KAFKA_SASL_USERNAME: &str = "kafka";
pub const KAFKA_SASL_PASSWORD: &str = "kafka123";
pub const KAFKA_SASL_MECHANISM: &str = "SCRAM-SHA-256";

const REDPANDA_IMAGE: &str = "docker.redpanda.com/redpandadata/redpanda:v26.1.6";
const KAFKA_CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(3);

#[instrument]
pub async fn start_kafka_docker_container(
    topics: &[&str],
) -> Result<(RunningContainer, FutureProducer), anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(KAFKA_DOCKER_CONTAINER)
        // Use Redpanda (Kafka-API compatible) as for dev/test purpose:
        // single binary (no JVM), fast startup, smaller CPU/RAM footprint
        // than apache/kafka - ideal for CI and local tests.
        .image(REDPANDA_IMAGE.to_string())
        // Docker owns the published port before the broker advertises it. The
        // internal listener serves in-container rpk; the external listener serves clients.
        .entrypoint(["/bin/sh", "-c"])
        .command([
            format!(r#"while [ ! -f /tmp/kafka-host-port ]; do sleep 0.1; done; exec rpk redpanda start --set redpanda.enable_sasl=true --set 'redpanda.superusers=["{KAFKA_SASL_USERNAME}"]' --smp 1 --overprovisioned --node-id 0 --mode dev-container --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 --advertise-kafka-addr internal://127.0.0.1:9092,external://127.0.0.1:$(cat /tmp/kafka-host-port)"#),
        ])
        .publish_port(19092)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "rpk cluster health | grep -E 'Healthy:.+true' || exit 1".to_string(),
            ]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(60),
            start_period: Some(10_000_000_000), // 10s
            start_interval: None,
        })
        .build()?
        .start()
        .await?;
    let port = running_container.host_port(19092)?;
    running_container
        .exec([
            "sh",
            "-c",
            &format!(
                "printf '%s' '{port}' > /tmp/kafka-host-port.tmp && mv /tmp/kafka-host-port.tmp /tmp/kafka-host-port"
            ),
        ])
        .await?;
    running_container
        .wait_healthy(Some(KAFKA_CONTAINER_START_TIMEOUT))
        .await?;

    tracing::debug!("Kafka user creation command result: {}", running_container.exec_cmd(
        &format!("rpk acl user create {KAFKA_SASL_USERNAME} -p {KAFKA_SASL_PASSWORD} --mechanism {KAFKA_SASL_MECHANISM} -X brokers=localhost:9092"),
    )
    .await?);

    for topic in topics {
        tracing::debug!(
            "Kafka topic '{topic}' creation command result: {}",
            running_container
                .exec_cmd(&format!(
                    "rpk topic create {topic} \
                --brokers localhost:9092 \
                --user {KAFKA_SASL_USERNAME} \
                --password {KAFKA_SASL_PASSWORD} \
                --sasl-mechanism {KAFKA_SASL_MECHANISM}"
                ),)
                .await?
        );
    }

    let producer = create_kafka_producer(
        &format!("localhost:{port}"),
        Some(KAFKA_SASL_USERNAME),
        Some(KAFKA_SASL_PASSWORD),
    )?;

    // Verify broker is ready to accept connections by fetching metadata
    verify_broker_ready(&producer, topics, port).await?;

    Ok((running_container, producer))
}

pub fn create_kafka_producer(
    broker: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<FutureProducer, anyhow::Error> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "30000");

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

/// Verify that the Kafka broker is ready to accept connections by fetching metadata.
/// This helps avoid race conditions where the container is "healthy" but SASL auth
/// isn't fully initialized yet.
async fn verify_broker_ready(
    producer: &FutureProducer,
    topics: &[&str],
    port: u16,
) -> Result<(), anyhow::Error> {
    const MAX_RETRIES: u32 = 10;
    const RETRY_DELAY: Duration = Duration::from_secs(1);
    const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

    for attempt in 1..=MAX_RETRIES {
        match producer.client().fetch_metadata(None, METADATA_TIMEOUT) {
            Ok(metadata) => {
                anyhow::ensure!(
                    !metadata.brokers().is_empty()
                        && metadata.brokers().iter().all(|broker| {
                            broker.host() == "127.0.0.1" && broker.port() == i32::from(port)
                        }),
                    "Kafka metadata must advertise this fixture's published endpoint 127.0.0.1:{port}"
                );
                // Verify that all expected topics exist
                let available_topics: Vec<&str> = metadata
                    .topics()
                    .iter()
                    .map(rdkafka::metadata::MetadataTopic::name)
                    .collect();
                let missing_topics: Vec<&str> = topics
                    .iter()
                    .filter(|t| !available_topics.contains(t))
                    .copied()
                    .collect();

                if missing_topics.is_empty() {
                    tracing::debug!(
                        "Broker ready at 127.0.0.1:{port}: found {} brokers and {} topics",
                        metadata.brokers().len(),
                        metadata.topics().len()
                    );
                    return Ok(());
                }

                tracing::debug!(
                    "Broker metadata fetched but missing topics {:?} (attempt {}/{})",
                    missing_topics,
                    attempt,
                    MAX_RETRIES
                );
            }
            Err(e) => {
                tracing::debug!(
                    "Failed to fetch broker metadata (attempt {}/{}): {}",
                    attempt,
                    MAX_RETRIES,
                    e
                );
            }
        }

        if attempt < MAX_RETRIES {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    Err(anyhow::anyhow!(
        "Failed to verify broker readiness after {MAX_RETRIES} attempts"
    ))
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
    const DELAY_S: u64 = 2;
    const QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Send a tombstone (null payload) message to a Kafka topic.
pub async fn send_tombstone_to_kafka(
    producer: &FutureProducer,
    topic: &str,
    partition: i32,
    key: &str,
) -> Result<(), anyhow::Error> {
    const MAX_RETRIES: u32 = 5;
    const DELAY_S: u64 = 2;
    const QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

    let key_owned = key.to_string();
    let mut last_error = None;
    for attempt in 0..=MAX_RETRIES {
        let record: FutureRecord<'_, String, ()> =
            FutureRecord::to(topic).partition(partition).key(&key_owned);
        match producer.send(record, QUEUE_TIMEOUT).await {
            Ok(_) => {
                if attempt > 0 {
                    tracing::debug!("Tombstone sent successfully after {attempt} retries");
                }
                last_error = None;
                break;
            }
            Err((e, _)) if attempt < MAX_RETRIES => {
                tracing::debug!(
                    "Kafka tombstone send failed (attempt {}/{}): {e}. Retrying in {DELAY_S} seconds",
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
            "Kafka tombstone delivery failed after {} attempts: {e}",
            MAX_RETRIES + 1,
        )));
    }
    Ok(())
}

/// Send a single JSON message to a specific Kafka partition with a custom timestamp.
pub async fn send_message_to_kafka_partition(
    producer: &FutureProducer,
    topic: &str,
    partition: i32,
    timestamp: i64,
    message: &serde_json::Value,
) -> Result<(), anyhow::Error> {
    const MAX_RETRIES: u32 = 5;
    const DELAY_S: u64 = 2;
    const QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

    let message_str = serde_json::to_string(message)?;
    let mut last_error = None;

    for attempt in 0..=MAX_RETRIES {
        let record: FutureRecord<'_, String, String> = FutureRecord::to(topic)
            .partition(partition)
            .payload(&message_str)
            .timestamp(timestamp);

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
    Ok(())
}

/// Create a Kafka topic with a specific number of partitions.
pub async fn create_kafka_topic_with_partitions(
    running_container: &crate::docker::RunningContainer,
    port: u16,
    topic: &str,
    partitions: i32,
) -> Result<(), anyhow::Error> {
    let output = running_container
        .exec_cmd(&format!(
            "rpk topic create {topic} \
            --partitions {partitions} \
            --brokers localhost:9092 \
            --user {KAFKA_SASL_USERNAME} \
            --password {KAFKA_SASL_PASSWORD} \
            --sasl-mechanism {KAFKA_SASL_MECHANISM}"
        ))
        .await?;
    tracing::debug!("Created topic '{topic}' with {partitions} partitions: {output}");

    let producer = create_kafka_producer(
        &format!("localhost:{port}"),
        Some(KAFKA_SASL_USERNAME),
        Some(KAFKA_SASL_PASSWORD),
    )?;
    wait_for_topic_partitions(&producer, topic, partitions).await?;

    Ok(())
}

/// Wait until topic metadata reports the expected partition count.
async fn wait_for_topic_partitions(
    producer: &FutureProducer,
    topic: &str,
    expected_partitions: i32,
) -> Result<(), anyhow::Error> {
    const MAX_RETRIES: u32 = 10;
    const RETRY_DELAY: Duration = Duration::from_millis(250);
    const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

    for attempt in 1..=MAX_RETRIES {
        match producer
            .client()
            .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        {
            Ok(metadata) => {
                let partition_count = metadata
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .map_or(0, |t| t.partitions().len());

                if i32::try_from(partition_count).unwrap_or(0) == expected_partitions {
                    tracing::debug!("Topic '{topic}' ready with {expected_partitions} partitions");
                    return Ok(());
                }

                tracing::debug!(
                    "Topic '{topic}' has {partition_count} partitions, expected {expected_partitions} (attempt {attempt}/{MAX_RETRIES})"
                );
            }
            Err(e) => {
                tracing::debug!(
                    "Failed to fetch metadata for topic '{topic}' (attempt {attempt}/{MAX_RETRIES}): {e}"
                );
            }
        }

        if attempt < MAX_RETRIES {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    Err(anyhow::anyhow!(
        "Topic '{topic}' did not become ready with {expected_partitions} partitions after {MAX_RETRIES} attempts"
    ))
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
