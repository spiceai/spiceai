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

use arrow::datatypes::Schema;
use arrow::{datatypes::SchemaRef, json::ReaderBuilder};
use datafusion::common::project_schema;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
};
use futures::Stream;
use rdkafka::{
    ClientConfig, Message, Offset,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    util::get_rdkafka_version,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{any::Any, sync::Arc};
use tokio_stream::StreamExt;
use tonic::async_trait;

use crate::cdc::{self, ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError};

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

    #[snafu(display("Received empty batch. Retry"))]
    EmptyBatch,
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
    #[expect(clippy::cast_sign_loss)]
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
        self.consumer.stream().filter_map(move |msg| {
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

    pub fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.consumer
            .store_offset(topic, partition, offset)
            .context(UnableToCommitMessageSnafu)
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
        tracing::debug!("Using kafka group_id: {}", group_id);

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
            .set("security.protocol", &kafka_config.security_protocol);

        if kafka_config.security_protocol.to_lowercase() != "plaintext" {
            config.set("sasl.mechanism", &kafka_config.sasl_mechanism);
        }

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

    pub fn topic(&self) -> &str {
        self.msg.topic()
    }

    pub fn partition(&self) -> i32 {
        self.msg.partition()
    }

    pub fn offset(&self) -> i64 {
        self.msg.offset()
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

pub struct MessageBatchCommitter {
    consumer: &'static KafkaConsumer,
    offsets: Vec<(String, i32, i64)>,
}

impl MessageBatchCommitter {
    pub fn from_messages<K, V>(
        consumer: &'static KafkaConsumer,
        messages: &[KafkaMessage<'_, K, V>],
    ) -> Self {
        let mut max_offsets: HashMap<(String, i32), i64> = HashMap::new();

        for msg in messages {
            let key = (msg.topic().to_string(), msg.partition());
            max_offsets
                .entry(key)
                .and_modify(|existing| {
                    if msg.offset() > *existing {
                        *existing = msg.offset();
                    }
                })
                .or_insert(msg.offset());
        }

        let offsets = max_offsets
            .into_iter()
            .map(|((topic, partition), offset)| (topic, partition, offset))
            .collect();

        Self { consumer, offsets }
    }
}

impl CommitChange for MessageBatchCommitter {
    fn commit(&self) -> Result<(), CommitError> {
        for (topic, partition, offset) in &self.offsets {
            self.consumer
                .store_offset(topic, *partition, *offset)
                .boxed()
                .map_err(|e| CommitError::UnableToCommitChange { source: e })?;
        }
        Ok(())
    }
}

pub struct Kafka {
    schema: SchemaRef,
    consumer: &'static KafkaConsumer,
    flatten_json: Option<String>,
    batching: (usize, Duration),
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
            batching: (10000, Duration::from_secs(1)),
        }
    }

    #[must_use]
    pub fn with_flatten_json(mut self, flatten_json: Option<String>) -> Self {
        self.flatten_json = flatten_json;
        self
    }

    #[must_use]
    pub fn with_batching(mut self, batching: (usize, Duration)) -> Self {
        self.batching = batching;
        self
    }

    #[must_use]
    pub fn stream_changes(&self) -> ChangesStream {
        let schema = Arc::clone(&self.schema);
        let flatten_json = self.flatten_json.clone();
        let consumer = self.consumer;
        let stream = self
            .consumer
            .stream_json::<serde_json::Value, serde_json::Value>()
            .chunks_timeout(self.batching.0, self.batching.1)
            .map(move |msgs| {
                let schema = Arc::clone(&schema);

                // Collect all successful messages, fail on first error
                let messages: Vec<_> = msgs
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(cdc::StreamError::Kafka)?;

                if messages.is_empty() {
                    return Err(cdc::StreamError::Kafka(Error::EmptyBatch));
                }

                let change_batch = values_to_change_batch(
                    messages.iter().map(KafkaMessage::value),
                    flatten_json.as_ref(),
                    &schema,
                );

                let committer = MessageBatchCommitter::from_messages(consumer, &messages);

                change_batch.map(|rb| ChangeEnvelope::new(Box::new(committer), rb, true))
            });

        Box::pin(stream)
    }
}

fn values_to_change_batch<'a>(
    values: impl Iterator<Item = &'a Value>,
    flatten_json: Option<&String>,
    schema: &Arc<Schema>,
) -> Result<ChangeBatch, cdc::StreamError> {
    // Build newline-delimited JSON from all values
    let json_str: String = values
        .map(|value| match flatten_json {
            Some(delimiter) => dataformat_json::flatten_json_obj(value, delimiter).to_string(),
            None => value.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Convert JSON string to Arrow record batch (ReaderBuilder handles NDJSON)
    let rb = ReaderBuilder::new(Arc::clone(schema))
        .build(std::io::Cursor::new(json_str.as_bytes()))
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?
        .next()
        .transpose()
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?
        .ok_or_else(|| {
            cdc::StreamError::Arrow("No record batch found in JSON message".to_string())
        })?;

    cdc::wrap_data_as_change_batch(schema, &rb)
        .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_schema_with_nullable() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]))
    }

    #[test]
    fn test_single_message() {
        let schema = test_schema();
        let values = [json!({"id": 1, "name": "alice"})];

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 1);
    }

    #[test]
    fn test_multiple_messages() {
        let schema = test_schema();
        let values = [
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
            json!({"id": 3, "name": "charlie"}),
        ];

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 3);
    }

    #[test]
    fn test_empty_messages() {
        let schema = test_schema();
        let values: Vec<serde_json::Value> = vec![];

        let result = values_to_change_batch(values.iter(), None, &schema);

        match result {
            Err(cdc::StreamError::Arrow(msg)) => {
                assert!(msg.contains("No record batch found"));
            }
            _ => panic!("Expected Arrow error"),
        }
    }

    #[test]
    fn test_with_null_fields() {
        let schema = test_schema_with_nullable();
        let values = [
            json!({"id": 1, "name": "alice", "age": 30}),
            json!({"id": 2, "name": null, "age": null}),
            json!({"id": 3, "name": "charlie", "age": 25}),
        ];

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 3);
    }

    #[test]
    fn test_with_flatten_json() {
        // Schema expects flattened field names
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("address_city", DataType::Utf8, false),
            Field::new("address_zip", DataType::Utf8, false),
        ]));

        let values = [
            json!({"id": 1, "address": {"city": "NYC", "zip": "10001"}}),
            json!({"id": 2, "address": {"city": "LA", "zip": "90001"}}),
        ];

        let result = values_to_change_batch(values.iter(), Some(&"_".to_string()), &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 2);
    }

    #[test]
    fn test_schema_mismatch_returns_error() {
        let schema = test_schema(); // expects id (Int64), name (Utf8)
        let values = [json!({"wrong_field": "value"})];

        let result = values_to_change_batch(values.iter(), None, &schema);

        result.expect_err("error");
    }

    #[test]
    fn test_change_batch_has_correct_structure() {
        let schema = test_schema();
        let values = [
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
        ];

        let batch = values_to_change_batch(values.iter(), None, &schema).expect("batch");

        // ChangeBatch should have: op, primary_keys, data columns
        let record_batch = batch.record;
        assert_eq!(record_batch.num_columns(), 3);

        // Check op column has "c" for all rows
        let op_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("array");
        assert_eq!(op_col.value(0), "c");
        assert_eq!(op_col.value(1), "c");
    }

    #[test]
    fn test_large_batch() {
        let schema = test_schema();
        let values: Vec<Value> = (0..1000)
            .map(|i| json!({"id": i, "name": format!("user_{}", i)}))
            .collect();

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 1000);
    }
}
