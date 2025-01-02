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

use futures::{Stream, StreamExt};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    util::get_rdkafka_version,
    ClientConfig, Message, Offset,
};
use serde::de::DeserializeOwned;
use snafu::prelude::*;

use crate::cdc::{self, CommitChange, CommitError};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Kafka consumer: {source}"))]
    UnableToCreateConsumer { source: rdkafka::error::KafkaError },

    #[snafu(display("Unable to subscribe to Kafka topic '{topic}': {source}"))]
    UnableToSubscribeToTopic {
        topic: String,
        source: rdkafka::error::KafkaError,
    },

    #[snafu(display("Unable to receive message from Kafka: {source}"))]
    UnableToReceiveMessage { source: rdkafka::error::KafkaError },

    #[snafu(display("Unable to deserialize JSON message from Kafka: {source}"))]
    UnableToDeserializeJsonMessage { source: serde_json::Error },

    #[snafu(display("Unable to mark Kafka message as being processed: {source}"))]
    UnableToCommitMessage { source: rdkafka::error::KafkaError },

    #[snafu(display("Unable to restart Kafka offsets {message}: {source}"))]
    UnableToRestartTopic {
        source: rdkafka::error::KafkaError,
        message: String,
    },

    #[snafu(display("The metadata for topic {topic} was not found."))]
    MetadataTopicNotFound { topic: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SslIdentification {
    None,
    #[default]
    Https,
}

impl TryFrom<&str> for SslIdentification {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "none" => SslIdentification::None,
            "https" => SslIdentification::Https,
            _ => return Err(()),
        })
    }
}

impl std::fmt::Display for SslIdentification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslIdentification::None => write!(f, "none"),
            SslIdentification::Https => write!(f, "https"),
        }
    }
}

#[derive(Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub enable_ssl_certificate_verification: bool,
    pub ssl_endpoint_identification_algorithm: SslIdentification,
}

pub struct KafkaConsumer {
    group_id: String,
    consumer: StreamConsumer,
}

impl KafkaConsumer {
    pub fn create_with_existing_group_id(
        group_id: impl Into<String>,
        kafka_config: KafkaConfig,
    ) -> Result<Self> {
        Self::create(group_id.into(), kafka_config)
    }

    pub fn create_with_generated_group_id(
        dataset: &str,
        kafka_config: KafkaConfig,
    ) -> Result<Self> {
        Self::create(Self::generate_group_id(dataset), kafka_config)
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn subscribe(&self, topic: &str) -> Result<()> {
        self.consumer
            .subscribe(&[topic])
            .context(UnableToSubscribeToTopicSnafu { topic })
    }

    /// Receive a JSON message from the Kafka topic.
    pub async fn next_json<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
    ) -> Result<Option<KafkaMessage<K, V>>> {
        let mut stream = Box::pin(self.stream_json::<K, V>());
        stream.next().await.transpose()
    }

    /// Stream JSON messages from the Kafka topic.
    pub fn stream_json<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
    ) -> impl Stream<Item = Result<KafkaMessage<K, V>>> {
        self.consumer.stream().filter_map(move |msg| async move {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => return Some(Err(Error::UnableToReceiveMessage { source: e })),
            };

            let key_bytes = msg.key()?;
            let payload = msg.payload()?;

            let key = match serde_json::from_slice(key_bytes) {
                Ok(key) => key,
                Err(e) => return Some(Err(Error::UnableToDeserializeJsonMessage { source: e })),
            };

            let value = match serde_json::from_slice(payload) {
                Ok(value) => value,
                Err(e) => return Some(Err(Error::UnableToDeserializeJsonMessage { source: e })),
            };

            Some(Ok(KafkaMessage::new(&self.consumer, msg, key, value)))
        })
    }

    pub fn restart_topic(&self, topic: &str) -> Result<()> {
        let mut assignment = self
            .consumer
            .assignment()
            .context(UnableToRestartTopicSnafu {
                message: "Failed to get assignment".to_string(),
            })?;

        // Retrieve metadata for the topic to get the list of partitions
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), std::time::Duration::from_secs(1))
            .context(UnableToRestartTopicSnafu {
                message: "Failed to fetch metadata".to_string(),
            })?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .context(MetadataTopicNotFoundSnafu {
                topic: topic.to_string(),
            })?;

        // Assign each partition to start from the beginning
        for partition_metadata in topic_metadata.partitions() {
            tracing::debug!(
                "Resetting partition {} for topic {topic}",
                partition_metadata.id()
            );
            assignment
                .set_partition_offset(topic, partition_metadata.id(), Offset::Beginning)
                .context(UnableToRestartTopicSnafu {
                    message: "Failed to set partition in list".to_string(),
                })?;
            assignment = self
                .consumer
                .seek_partitions(assignment, std::time::Duration::from_secs(1))
                .context(UnableToRestartTopicSnafu {
                    message: "Failed to seek partitions".to_string(),
                })?;
        }

        self.consumer
            .store_offsets(&assignment)
            .context(UnableToRestartTopicSnafu {
                message: "Failed to commit".to_string(),
            })?;

        Ok(())
    }

    fn create(group_id: String, kafka_config: KafkaConfig) -> Result<Self> {
        let (_, version) = get_rdkafka_version();
        tracing::debug!("rd_kafka_version: {}", version);

        let mut config = ClientConfig::new();
        config
            .set("group.id", group_id.clone())
            .set("bootstrap.servers", kafka_config.brokers)
            // For new consumer groups, start reading at the beginning of the topic
            .set("auto.offset.reset", "smallest")
            // Commit offsets automatically
            .set("enable.auto.commit", "true")
            // Commit offsets every 5 seconds
            .set("auto.commit.interval.ms", "5000")
            // Don't automatically store offsets the library provides to us - we will store them after processing explicitly
            // This is what gives us the "at least once" semantics
            .set("enable.auto.offset.store", "false")
            .set("security.protocol", kafka_config.security_protocol)
            .set("sasl.mechanism", kafka_config.sasl_mechanism);

        if let Some(sasl_username) = kafka_config.sasl_username {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = kafka_config.sasl_password {
            config.set("sasl.password", sasl_password);
        }
        if let Some(ssl_ca_location) = kafka_config.ssl_ca_location {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if kafka_config.enable_ssl_certificate_verification {
            config.set("enable.ssl.certificate.verification", "true");
        } else {
            config.set("enable.ssl.certificate.verification", "false");
        }
        config.set(
            "ssl.endpoint.identification.algorithm",
            kafka_config
                .ssl_endpoint_identification_algorithm
                .to_string(),
        );

        let consumer: StreamConsumer = config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .context(UnableToCreateConsumerSnafu)?;

        Ok(Self { group_id, consumer })
    }

    fn generate_group_id(dataset: &str) -> String {
        format!("spice.ai-{dataset}-{}", uuid::Uuid::new_v4())
    }
}

pub struct KafkaMessage<'a, K, V> {
    consumer: &'a StreamConsumer,
    msg: BorrowedMessage<'a>,
    key: K,
    value: V,
}

impl<'a, K, V> KafkaMessage<'a, K, V> {
    fn new(consumer: &'a StreamConsumer, msg: BorrowedMessage<'a>, key: K, value: V) -> Self {
        Self {
            consumer,
            msg,
            key,
            value,
        }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn mark_processed(&self) -> Result<()> {
        self.consumer
            .store_offset_from_message(&self.msg)
            .context(UnableToCommitMessageSnafu)
    }
}

impl<K, V> CommitChange for KafkaMessage<'_, K, V> {
    fn commit(&self) -> Result<(), CommitError> {
        self.mark_processed()
            .boxed()
            .map_err(|e| cdc::CommitError::UnableToCommitChange { source: e })?;
        Ok(())
    }
}
