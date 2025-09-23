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

use std::sync::atomic::{AtomicU64, Ordering};
use std::{any::Any, sync::Arc};

use arrow::{datatypes::SchemaRef, json::ReaderBuilder};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
};
use futures::{Stream, StreamExt};
use rdkafka::{
    ClientConfig, Message, Offset,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    util::get_rdkafka_version,
};
use serde::de::DeserializeOwned;
use snafu::prelude::*;
use tonic::async_trait;

use crate::cdc::{self, ChangeEnvelope, ChangesStream, CommitChange, CommitError};

pub use rdkafka;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
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
    pub consumer_group_id: Option<String>,
    pub metrics_store: Option<Arc<KafkaMetrics>>,
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("brokers", &self.brokers)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field(
                "sasl_password",
                &self.sasl_password.as_ref().map(|_| "REDACTED"),
            )
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field(
                "enable_ssl_certificate_verification",
                &self.enable_ssl_certificate_verification,
            )
            .field(
                "ssl_endpoint_identification_algorithm",
                &self.ssl_endpoint_identification_algorithm,
            )
            .field("consumer_group_id", &self.consumer_group_id)
            .field(
                "metrics_store",
                &self.metrics_store.as_ref().map(|_| "Some(KafkaMetrics)"),
            )
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct KafkaMetrics {
    /// Total consumer lag across all partitions
    pub records_lag: AtomicU64,
    /// Total number of messages consumed
    pub records_consumed: AtomicU64,
    /// Total bytes consumed
    pub bytes_consumed: AtomicU64,
}

struct KafkaConsumerContext {
    metrics: Arc<KafkaMetrics>,
}

impl KafkaConsumerContext {
    fn new(metrics: Arc<KafkaMetrics>) -> Self {
        Self { metrics }
    }
}

impl KafkaMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_records_lag(&self, lag: u64) {
        self.records_lag.store(lag, Ordering::Relaxed);
    }

    pub fn update_records_consumed(&self, count: u64) {
        self.records_consumed.store(count, Ordering::Relaxed);
    }

    pub fn update_bytes_consumed(&self, bytes: u64) {
        self.bytes_consumed.store(bytes, Ordering::Relaxed);
    }
}

impl rdkafka::ClientContext for KafkaConsumerContext {
    #[allow(clippy::cast_sign_loss)]
    fn stats(&self, statistics: rdkafka::Statistics) {
        // Calculate total consumer lag from all topic partitions
        let mut total_lag = 0u64;
        let mut has_valid_partitions = false;

        for topic in statistics.topics.values() {
            for partition in topic.partitions.values() {
                // Skip internal partitions (partition id -1), and only consider partitions with known lag (-1 means unknown)
                if partition.partition >= 0 && partition.consumer_lag >= 0 {
                    total_lag += partition.consumer_lag as u64;
                    has_valid_partitions = true;
                }
            }
        }

        // Update total lag only if we have valid partitions to avoid misleading data
        if has_valid_partitions {
            self.metrics.update_records_lag(total_lag);
        }

        self.metrics
            .update_records_consumed(statistics.rxmsgs as u64);
        self.metrics
            .update_bytes_consumed(statistics.rxmsg_bytes as u64);

        tracing::trace!(
            "Kafka metrics updated for consumer: {}, topics: {:?}, lag: {}, messages: {}, bytes: {}, brokers={:?}, consumer_group_state={:?}",
            statistics.name,
            statistics.topics.keys().collect::<Vec<_>>(),
            total_lag,
            statistics.rxmsgs,
            statistics.rxmsg_bytes,
            statistics
                .brokers
                .values()
                .map(|b| format!("{}:{}", b.name, b.state))
                .collect::<Vec<_>>(),
            statistics.cgrp.as_ref().map(|cgrp| &cgrp.state),
        );
    }
}

impl rdkafka::consumer::ConsumerContext for KafkaConsumerContext {}

pub struct KafkaConsumer {
    group_id: String,
    consumer: StreamConsumer<KafkaConsumerContext>,
    metrics: Arc<KafkaMetrics>,
}

impl KafkaConsumer {
    pub fn create_with_existing_group_id(
        group_id: impl Into<String>,
        kafka_config: &KafkaConfig,
    ) -> Result<Self> {
        Self::create(group_id.into(), kafka_config)
    }

    pub fn create_for_dataset(
        dataset: &str,
        group_id: Option<String>,
        kafka_config: &KafkaConfig,
    ) -> Result<Self> {
        Self::create(
            group_id.unwrap_or_else(|| Self::generate_group_id(dataset)),
            kafka_config,
        )
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
    ) -> Result<Option<KafkaMessage<'_, K, V>>> {
        let mut stream = Box::pin(self.stream_json::<K, V>());
        stream.next().await.transpose()
    }

    /// Stream JSON messages from the Kafka topic.
    pub fn stream_json<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
    ) -> impl Stream<Item = Result<KafkaMessage<'_, K, V>>> {
        self.consumer.stream().filter_map(move |msg| async move {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => return Some(Err(Error::UnableToReceiveMessage { source: e })),
            };

            let key = match msg.key() {
                Some(key_bytes) => match serde_json::from_slice(key_bytes) {
                    Ok(key) => Some(key),
                    Err(e) => {
                        return Some(Err(Error::UnableToDeserializeJsonMessage { source: e }));
                    }
                },
                None => None,
            };

            let payload = msg.payload()?;
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

    #[must_use]
    pub fn metrics(&self) -> &Arc<KafkaMetrics> {
        &self.metrics
    }

    fn create(group_id: String, kafka_config: &KafkaConfig) -> Result<Self> {
        let (_, version) = get_rdkafka_version();
        tracing::debug!("rd_kafka_version: {}", version);

        let mut config = ClientConfig::new();
        config
            .set("group.id", group_id.clone())
            .set("bootstrap.servers", &kafka_config.brokers)
            // Explicit statistics emission interval configuration (1s is the default)
            .set("statistics.interval.ms", "1000")
            .set("retry.backoff.ms", "1000")
            .set("retry.backoff.max.ms", "30000")
            .set("reconnect.backoff.ms", "1000")
            .set("reconnect.backoff.max.ms", "30000")
            .set("debug", "broker,cgrp,fetch")
            // For new consumer groups, start reading at the beginning of the topic
            .set("auto.offset.reset", "smallest")
            // Commit offsets automatically
            .set("enable.auto.commit", "true")
            // Commit offsets every 5 seconds
            .set("auto.commit.interval.ms", "5000")
            // Don't automatically store offsets the library provides to us - we will store them after processing explicitly
            // This is what gives us the "at least once" semantics
            .set("enable.auto.offset.store", "false")
            .set("security.protocol", &kafka_config.security_protocol)
            .set("sasl.mechanism", &kafka_config.sasl_mechanism);

        if let Some(sasl_username) = &kafka_config.sasl_username {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = &kafka_config.sasl_password {
            config.set("sasl.password", sasl_password);
        }
        if let Some(ssl_ca_location) = &kafka_config.ssl_ca_location {
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

        let metrics = kafka_config
            .metrics_store
            .clone()
            .unwrap_or(Arc::new(KafkaMetrics::new()));

        let consumer: StreamConsumer<KafkaConsumerContext> = config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(KafkaConsumerContext::new(Arc::clone(&metrics)))
            .context(UnableToCreateConsumerSnafu)?;

        Ok(Self {
            group_id,
            consumer,
            metrics,
        })
    }

    fn generate_group_id(dataset: &str) -> String {
        format!("spice.ai-{dataset}-{}", uuid::Uuid::new_v4())
    }
}

pub struct KafkaMessage<'a, K, V> {
    consumer: &'a StreamConsumer<KafkaConsumerContext>,
    msg: BorrowedMessage<'a>,
    key: Option<K>,
    value: V,
}

impl<'a, K, V> KafkaMessage<'a, K, V> {
    fn new(
        consumer: &'a StreamConsumer<KafkaConsumerContext>,
        msg: BorrowedMessage<'a>,
        key: Option<K>,
        value: V,
    ) -> Self {
        Self {
            consumer,
            msg,
            key,
            value,
        }
    }

    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
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

pub struct Kafka {
    schema: SchemaRef,
    consumer: &'static KafkaConsumer,
    flatten_json: Option<String>,
}

impl std::fmt::Debug for Kafka {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kafka")
            .field("schema", &self.schema)
            .field("consumer_group_id", &self.consumer.group_id())
            .field("flatten_json", &self.flatten_json)
            .finish_non_exhaustive()
    }
}

impl Kafka {
    #[must_use]
    pub fn new(schema: SchemaRef, consumer: KafkaConsumer) -> Self {
        Self {
            schema,
            consumer: Box::leak(Box::new(consumer)),
            flatten_json: None,
        }
    }

    #[must_use]
    pub fn with_flatten_json(mut self, flatten_json: Option<String>) -> Self {
        self.flatten_json = flatten_json;
        self
    }

    #[must_use]
    pub fn stream_changes(&self) -> ChangesStream {
        let schema = Arc::clone(&self.schema);
        let flatten_json = self.flatten_json.clone();
        let stream = self
            .consumer
            .stream_json::<serde_json::Value, serde_json::Value>()
            .map(move |msg| {
                let schema = Arc::clone(&schema);
                let msg = msg.map_err(cdc::StreamError::Kafka)?;

                let json_str = match flatten_json {
                    Some(ref delimiter) => {
                        dataformat_json::flatten_json_obj(msg.value(), delimiter).to_string()
                    }
                    None => msg.value().to_string(),
                };

                // convert JSON string to Arrow record batch
                let rb = ReaderBuilder::new(Arc::clone(&schema))
                    .build(std::io::Cursor::new(json_str.as_bytes()))
                    .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?
                    .next()
                    .transpose()
                    .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?
                    .ok_or_else(|| {
                        cdc::StreamError::Arrow("No record batch found in JSON message".to_string())
                    })?;

                // Wrap the record batch to emulate a change event
                cdc::wrap_data_as_change_batch(&schema, &rb)
                    .map(|rb| ChangeEnvelope::new(Box::new(msg), rb))
                    .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))
            });

        Box::pin(stream)
    }
}

#[async_trait]
impl TableProvider for Kafka {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))) as Arc<dyn ExecutionPlan>)
    }
}
