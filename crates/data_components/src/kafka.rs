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
    consumer::{BaseConsumer, CommitMode, Consumer, Rebalance, StreamConsumer},
    message::BorrowedMessage,
    topic_partition_list::TopicPartitionList,
    util::get_rdkafka_version,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::{any::Any, sync::Arc};
use tokio::sync::Notify;
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

    #[snafu(display("Unable to commit Kafka consumer state: {source}"))]
    UnableToCommitConsumerState { source: rdkafka::error::KafkaError },

    #[snafu(display("Unable to restart Kafka offsets {message}: {source}"))]
    UnableToRestartTopic {
        source: rdkafka::error::KafkaError,
        message: String,
    },

    #[snafu(display("Unable to restore Kafka offsets {message}: {source}"))]
    UnableToRestoreOffsets {
        source: rdkafka::error::KafkaError,
        message: String,
    },

    #[snafu(display("The metadata for topic {topic} was not found."))]
    MetadataTopicNotFound { topic: String },

    #[snafu(display("Received empty batch from Kafka topic. The consumer will retry."))]
    EmptyBatch,

    #[snafu(display(
        "Received Kafka message without payload from topic '{topic}', partition {partition}, offset {offset}"
    ))]
    MessageMissingPayload {
        topic: String,
        partition: i32,
        offset: i64,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KafkaOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl KafkaOffset {
    #[must_use]
    pub fn next_read_offset(&self) -> i64 {
        self.offset.saturating_add(1)
    }
}

#[async_trait]
pub trait KafkaOffsetCommitHook: Send + Sync {
    /// Runs after the refresh task has written a batch but before Kafka offsets are committed.
    /// If this hook fails, Kafka is left uncommitted; plain append accelerations may replay the
    /// batch after restart and should be treated as at-least-once.
    async fn commit_offsets(&self, offsets: &[KafkaOffset])
    -> std::result::Result<(), CommitError>;
}

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
    /// Set to true the first time the rdkafka stats callback fires with at
    /// least one valid partition. Until this flips, `records_lag == 0` only
    /// means "we haven't observed any stats yet", not "the consumer is
    /// caught up".
    pub has_received_stats: AtomicBool,
    /// Notified by the stats callback whenever the consumer is observed to
    /// be caught up (received at least one valid stats sample with
    /// `total_lag == 0`). Used by CDC connectors to emit a synthetic
    /// ready-signal envelope on quiet topics without polling.
    pub caught_up: Notify,
}

struct KafkaConsumerContext {
    metrics: Arc<KafkaMetrics>,
    /// Offsets to seek to on first partition assignment (restored from sidecar).
    /// Wrapped in a Mutex so the callback can take them (consumed once).
    restore_offsets: std::sync::Mutex<Option<TopicPartitionList>>,
}

impl KafkaConsumerContext {
    fn new(metrics: Arc<KafkaMetrics>) -> Self {
        Self {
            metrics,
            restore_offsets: std::sync::Mutex::new(None),
        }
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

    /// Returns true once the consumer has received at least one statistics
    /// callback that reported valid partitions and the most recent total lag
    /// across those partitions is zero.
    ///
    /// Uses Acquire ordering on `has_received_stats` so that an observer that
    /// sees the flag set is guaranteed to also see the matching Release-stored
    /// `records_lag` value from the same stats callback (and not a stale
    /// default 0).
    ///
    /// Used by CDC connectors to decide when to emit a synthetic
    /// `is_dataset_ready=true` envelope on quiet topics. See the
    /// [`crate::cdc::ChangesStream`] readiness contract.
    #[must_use]
    pub fn is_caught_up(&self) -> bool {
        self.has_received_stats.load(Ordering::Acquire)
            && self.records_lag.load(Ordering::Relaxed) == 0
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
            // Pair these stores with the Acquire load in `is_caught_up`:
            // store the lag first (Relaxed is sufficient since the Release
            // store below acts as the release fence), then publish the
            // "stats received" flag with Release so any observer that sees
            // the flag set also sees this exact lag value.
            self.metrics.update_records_lag(total_lag);
            self.metrics
                .has_received_stats
                .store(true, Ordering::Release);
            if total_lag == 0 {
                // Wake any task waiting on `KafkaMetrics::caught_up` (e.g.
                // the CDC ready-signal wrapper). Cheap and idempotent: if no
                // one is waiting, the permit is stored for the next waiter.
                self.metrics.caught_up.notify_one();
            }
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

impl rdkafka::consumer::ConsumerContext for KafkaConsumerContext {
    fn post_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(_) = rebalance {
            // On first assignment after subscribe, seek to sidecar offsets if available.
            // Take the offsets so this only fires once.
            let offsets = self
                .restore_offsets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();

            if let Some(tpl) = offsets {
                match base_consumer.seek_partitions(tpl, Duration::from_secs(5)) {
                    Ok(_) => {
                        tracing::info!("Restored Kafka consumer offsets from sidecar");
                    }
                    Err(e) => {
                        tracing::error!("Failed to seek to restored offsets: {e}");
                    }
                }
            }
        }
    }
}

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

    pub fn commit_stored_offsets(&self) -> Result<()> {
        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .context(UnableToCommitConsumerStateSnafu)
    }

    pub fn restore_offsets(&self, offsets: &[KafkaOffset]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let mut topic_partition_list = TopicPartitionList::new();
        for offset in offsets {
            topic_partition_list
                .add_partition_offset(
                    &offset.topic,
                    offset.partition,
                    Offset::Offset(offset.next_read_offset()),
                )
                .context(UnableToRestoreOffsetsSnafu {
                    message: "Failed to build topic partition list".to_string(),
                })?;
        }

        // Store offsets in the consumer context. On the next rebalance (partition
        // assignment after subscribe), the post_rebalance callback will seek to
        // these offsets. This avoids committing to the group coordinator (which
        // fails with UnknownMemberId on restart) while keeping group membership
        // intact for rebalancing and offset commits during normal operation.
        let context = self.consumer.context();
        *context
            .restore_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(topic_partition_list);

        Ok(())
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
            // Commit offsets only after Spice has written the batch and persisted the sidecar cursor.
            .set("enable.auto.commit", "false")
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

    /// Fetch the latest message from a Kafka topic without affecting any existing
    /// consumer group state.
    ///
    /// Creates a temporary consumer, seeks to the latest available message across
    /// all partitions, reads it, and returns the owned key/value pair.
    pub async fn fetch_latest_message<K: DeserializeOwned, V: DeserializeOwned>(
        topic: &str,
        kafka_config: &KafkaConfig,
        timeout: Duration,
    ) -> Result<Option<(Option<K>, V)>> {
        let temp_group_id = format!("spice-schema-peek-{}", uuid::Uuid::new_v4());
        let mut peek_config = kafka_config.clone();
        peek_config.metrics_store = None; // Avoid skewing real consumer metrics
        let temp_consumer = Self::create(temp_group_id, &peek_config)?;

        // Fetch topic metadata to discover partitions
        let metadata = temp_consumer
            .consumer
            .fetch_metadata(Some(topic), timeout)
            .context(UnableToRestartTopicSnafu {
                message: "Failed to fetch topic metadata".to_string(),
            })?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .context(MetadataTopicNotFoundSnafu {
                topic: topic.to_string(),
            })?;

        // Find the partition with the highest watermark (most recent data)
        let mut best_partition: Option<(i32, i64)> = None;
        for partition in topic_metadata.partitions() {
            let (low, high) = temp_consumer
                .consumer
                .fetch_watermarks(topic, partition.id(), timeout)
                .context(UnableToRestartTopicSnafu {
                    message: format!(
                        "Failed to fetch watermarks for partition {}",
                        partition.id()
                    ),
                })?;

            if high > low {
                match &best_partition {
                    Some((_, best_high)) if high <= *best_high => {}
                    _ => best_partition = Some((partition.id(), high)),
                }
            }
        }

        let Some((partition_id, high_watermark)) = best_partition else {
            return Ok(None); // No messages available
        };

        // Manually assign the consumer to read from the latest offset
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition_id, Offset::Offset(high_watermark - 1))
            .context(UnableToRestartTopicSnafu {
                message: "Failed to configure partition offset".to_string(),
            })?;

        temp_consumer
            .consumer
            .assign(&tpl)
            .context(UnableToRestartTopicSnafu {
                message: "Failed to assign partition".to_string(),
            })?;

        // Read the message with a timeout
        match tokio::time::timeout(timeout, temp_consumer.next_json::<K, V>()).await {
            Ok(Ok(Some(msg))) => Ok(Some(msg.into_key_value())),
            Ok(Ok(None)) | Err(_) => Ok(None),
            Ok(Err(e)) => Err(e),
        }
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

    /// Consume the message and return owned key/value data.
    pub fn into_key_value(self) -> (Option<K>, V) {
        (self.key, self.value)
    }
}

pub struct MessageBatchCommitter {
    consumer: &'static KafkaConsumer,
    offsets: Vec<KafkaOffset>,
    offset_commit_hook: Option<Arc<dyn KafkaOffsetCommitHook>>,
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
            .map(|((topic, partition), offset)| KafkaOffset {
                topic,
                partition,
                offset,
            })
            .collect();

        Self {
            consumer,
            offsets,
            offset_commit_hook: None,
        }
    }

    #[must_use]
    pub fn from_borrowed_messages(
        consumer: &'static KafkaConsumer,
        messages: &[BorrowedMessage<'_>],
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
            .map(|((topic, partition), offset)| KafkaOffset {
                topic,
                partition,
                offset,
            })
            .collect();

        Self {
            consumer,
            offsets,
            offset_commit_hook: None,
        }
    }

    #[must_use]
    pub fn with_offset_commit_hook(
        mut self,
        offset_commit_hook: Option<Arc<dyn KafkaOffsetCommitHook>>,
    ) -> Self {
        self.offset_commit_hook = offset_commit_hook;
        self
    }
}

#[async_trait]
impl CommitChange for MessageBatchCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        if let Some(offset_commit_hook) = &self.offset_commit_hook {
            offset_commit_hook.commit_offsets(&self.offsets).await?;
        }

        for offset in &self.offsets {
            self.consumer
                .store_offset(&offset.topic, offset.partition, offset.offset)
                .boxed()
                .map_err(|e| CommitError::UnableToCommitChange { source: e })?;
        }

        self.consumer
            .commit_stored_offsets()
            .boxed()
            .map_err(|e| CommitError::UnableToCommitChange { source: e })?;

        Ok(())
    }
}

pub struct Kafka {
    schema: SchemaRef,
    consumer: &'static KafkaConsumer,
    flatten_json: Option<String>,
    batching: (usize, Duration),
    offset_commit_hook: Option<Arc<dyn KafkaOffsetCommitHook>>,
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
            offset_commit_hook: None,
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
    pub fn with_offset_commit_hook(
        mut self,
        offset_commit_hook: Arc<dyn KafkaOffsetCommitHook>,
    ) -> Self {
        self.offset_commit_hook = Some(offset_commit_hook);
        self
    }

    #[must_use]
    pub fn stream_changes(&self) -> ChangesStream {
        let schema = Arc::clone(&self.schema);
        let flatten_json = self.flatten_json.clone();
        let consumer = self.consumer;
        let metrics = Arc::clone(self.consumer.metrics());
        let offset_commit_hook = self.offset_commit_hook.clone();
        let inner = self
            .consumer
            .consumer
            .stream()
            .chunks_timeout(self.batching.0, self.batching.1)
            .map(move |msgs| {
                let schema = Arc::clone(&schema);

                // Collect all successful messages, fail on first error
                let messages: Vec<_> = msgs
                    .into_iter()
                    .map(|msg| msg.context(UnableToReceiveMessageSnafu))
                    .collect::<Result<Vec<_>>>()
                    .map_err(cdc::StreamError::Kafka)?;

                if messages.is_empty() {
                    return Err(cdc::StreamError::Kafka(Error::EmptyBatch));
                }

                let change_batch =
                    messages_to_change_batch(&messages, flatten_json.as_ref(), &schema)?;

                let committer = MessageBatchCommitter::from_borrowed_messages(consumer, &messages)
                    .with_offset_commit_hook(offset_commit_hook.clone());

                Ok(ChangeEnvelope::new(Box::new(committer), change_batch, true))
            });

        Box::pin(inject_ready_signal_on_caught_up(
            inner,
            metrics,
            Arc::clone(&self.schema),
        ))
    }
}

fn messages_to_change_batch(
    messages: &[BorrowedMessage<'_>],
    flatten_json: Option<&String>,
    schema: &Arc<Schema>,
) -> Result<ChangeBatch, cdc::StreamError> {
    let payloads = messages
        .iter()
        .map(message_payload)
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(delimiter) = flatten_json {
        let values = payloads
            .into_iter()
            .map(|payload| {
                serde_json::from_slice::<Value>(payload).map_err(|e| {
                    cdc::StreamError::Kafka(Error::UnableToDeserializeJsonMessage { source: e })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return values_to_change_batch(values.iter(), Some(delimiter), schema);
    }

    payloads_to_change_batch(payloads.into_iter(), schema)
}

fn message_payload<'a>(message: &'a BorrowedMessage<'_>) -> Result<&'a [u8], cdc::StreamError> {
    message.payload().ok_or_else(|| {
        cdc::StreamError::Kafka(Error::MessageMissingPayload {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
        })
    })
}

fn payloads_to_change_batch<'a>(
    payloads: impl Iterator<Item = &'a [u8]>,
    schema: &Arc<Schema>,
) -> Result<ChangeBatch, cdc::StreamError> {
    let values = payloads
        .map(|payload| {
            serde_json::from_slice::<Value>(payload).map_err(|e| {
                cdc::StreamError::Kafka(Error::UnableToDeserializeJsonMessage { source: e })
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if values.is_empty() {
        return Err(cdc::StreamError::Arrow(
            "No Kafka message payload found in batch".to_string(),
        ));
    }

    values_to_change_batch(values.iter(), None, schema)
}

fn values_to_change_batch<'a>(
    values: impl Iterator<Item = &'a Value>,
    flatten_json: Option<&String>,
    schema: &Arc<Schema>,
) -> Result<ChangeBatch, cdc::StreamError> {
    // Build newline-delimited JSON from all values
    let json_values = values
        .map(|value| match flatten_json {
            Some(delimiter) => dataformat_json::flatten_json_obj(value, delimiter).to_string(),
            None => value.to_string(),
        })
        .collect::<Vec<_>>();
    let json_str = json_values.join("\n");

    json_bytes_to_change_batch(json_str.as_bytes(), schema)
}

fn json_bytes_to_change_batch(
    json: &[u8],
    schema: &Arc<Schema>,
) -> Result<ChangeBatch, cdc::StreamError> {
    // Convert JSON string to Arrow record batches (ReaderBuilder handles NDJSON).
    // The reader produces batches of up to batch_size rows. Collect all and concatenate
    // to avoid silently dropping rows beyond the first batch.
    let reader = ReaderBuilder::new(Arc::clone(schema))
        .build(std::io::Cursor::new(json))
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    let batches: Vec<_> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

    if batches.is_empty() {
        return Err(cdc::StreamError::Arrow(
            "No record batch found in JSON message".to_string(),
        ));
    }

    let rb = arrow::compute::concat_batches(schema, &batches)
        .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;

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

/// Wraps an inner Kafka-derived `ChangesStream` and emits a single synthetic
/// `is_dataset_ready=true` [`ChangeEnvelope`] (built via
/// [`cdc::build_ready_signal_envelope`]) once the underlying consumer reports
/// it has caught up to the source (`KafkaMetrics::caught_up` is notified by
/// the stats callback when `total_lag == 0`). The wrapper stops watching
/// after the first ready signal (whether emitted by the wrapper or carried
/// by a real change envelope).
///
/// This satisfies the [`cdc::ChangesStream`] readiness contract for quiet
/// topics where no actual change events are available to flip the
/// `is_dataset_ready` flag (e.g. on restart against an already-populated
/// accelerator). See <https://github.com/spiceai/spiceai/issues/5201>.
pub(crate) fn inject_ready_signal_on_caught_up<S>(
    inner: S,
    metrics: Arc<KafkaMetrics>,
    schema: SchemaRef,
) -> impl Stream<Item = Result<ChangeEnvelope, cdc::StreamError>>
where
    S: Stream<Item = Result<ChangeEnvelope, cdc::StreamError>> + Send + 'static,
{
    async_stream::stream! {
        let mut inner = Box::pin(inner);
        let mut ready_emitted = false;

        loop {
            tokio::select! {
                biased;
                next = inner.next() => match next {
                    Some(item) => {
                        if !ready_emitted
                            && let Ok(ref env) = item
                            && env.is_dataset_ready()
                        {
                            ready_emitted = true;
                        }
                        yield item;
                    }
                    None => break,
                },
                () = metrics.caught_up.notified(), if !ready_emitted => {
                    // Re-check under the same memory ordering as the
                    // notifier; `notified()` may wake spuriously if the
                    // notifier raced with another waiter.
                    if metrics.is_caught_up() {
                        match cdc::build_ready_signal_envelope(&schema) {
                            Ok(env) => {
                                ready_emitted = true;
                                tracing::debug!(
                                    "Kafka consumer reports zero lag; emitting synthetic ready signal envelope"
                                );
                                yield Ok(env);
                            }
                            Err(e) => {
                                // Building the envelope is schema-driven and
                                // therefore deterministic: a failure here
                                // will repeat on every wake-up. Surface it
                                // as a stream error and stop the synthetic
                                // ready-signal path so we don't spam logs
                                // forever — the inner stream still runs and
                                // can deliver readiness via a real envelope
                                // if any change event ever arrives.
                                tracing::error!(
                                    "Failed to build Kafka ready-signal envelope; \
                                     synthetic readiness disabled for this stream: {e}"
                                );
                                ready_emitted = true;
                                yield Err(cdc::StreamError::Arrow(format!(
                                    "failed to build Kafka ready-signal envelope: {e}"
                                )));
                            }
                        }
                    }
                }
            }
        }
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
    fn test_payloads_to_change_batch_accepts_pretty_json_messages() {
        let schema = test_schema();
        let first = br#"{
            "id": 1,
            "name": "alice"
        }"#;
        let second = br#"{
            "id": 2,
            "name": "bob"
        }"#;

        let result =
            payloads_to_change_batch([first.as_slice(), second.as_slice()].into_iter(), &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 2);
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

    /// Verifies that batches larger than the Arrow JSON reader's default batch size
    /// (1024 rows) are fully preserved. Before the fix, `.next()` only returned the
    /// first 1024-row batch and silently dropped the rest.
    #[test]
    fn test_batch_exceeding_default_reader_size() {
        let schema = test_schema();
        let values: Vec<Value> = (0..3000)
            .map(|i| json!({"id": i, "name": format!("user_{}", i)}))
            .collect();

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(
            batch.record.num_rows(),
            3000,
            "All 3000 rows should be preserved (was previously capped at 1024)"
        );
    }

    /// Verifies that exactly 1024 rows works (boundary of the default batch size).
    #[test]
    fn test_batch_at_default_reader_boundary() {
        let schema = test_schema();
        let values: Vec<Value> = (0..1024)
            .map(|i| json!({"id": i, "name": format!("user_{}", i)}))
            .collect();

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(batch.record.num_rows(), 1024);
    }

    /// Verifies that 1025 rows (one over the boundary) are all preserved.
    #[test]
    fn test_batch_one_over_default_reader_boundary() {
        let schema = test_schema();
        let values: Vec<Value> = (0..1025)
            .map(|i| json!({"id": i, "name": format!("user_{}", i)}))
            .collect();

        let result = values_to_change_batch(values.iter(), None, &schema);

        assert!(result.is_ok());
        let batch = result.expect("batch");
        assert_eq!(
            batch.record.num_rows(),
            1025,
            "1025 rows should not be truncated to 1024"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ready_signal_emitted_when_caught_up_on_quiet_stream() {
        // Inner stream that produces nothing (quiet topic).
        let inner = futures::stream::pending::<Result<ChangeEnvelope, cdc::StreamError>>();

        let metrics = Arc::new(KafkaMetrics::default());
        // Simulate a stats callback observing zero lag: flip the flag and
        // notify the wrapper.
        metrics
            .has_received_stats
            .store(true, std::sync::atomic::Ordering::Relaxed);
        metrics.caught_up.notify_one();

        let schema = test_schema();
        let stream = inject_ready_signal_on_caught_up(inner, Arc::clone(&metrics), schema);

        // Should yield a ready envelope essentially immediately.
        let next = tokio::time::timeout(Duration::from_secs(1), Box::pin(stream).next())
            .await
            .expect("ready envelope should be emitted promptly after notification")
            .expect("stream produced an item")
            .expect("item is Ok");

        assert!(next.is_dataset_ready(), "envelope must flag dataset ready");
        assert_eq!(
            next.change_batch.record.num_rows(),
            0,
            "ready signal envelope must carry zero rows"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ready_signal_not_emitted_before_stats_received() {
        let inner = futures::stream::pending::<Result<ChangeEnvelope, cdc::StreamError>>();

        let metrics = Arc::new(KafkaMetrics::default());
        // No stats callback yet — nothing notified, has_received_stats=false.

        let schema = test_schema();
        let stream = inject_ready_signal_on_caught_up(inner, Arc::clone(&metrics), schema);

        let res = tokio::time::timeout(Duration::from_millis(500), Box::pin(stream).next()).await;
        assert!(
            res.is_err(),
            "no ready envelope must be emitted before stats are received"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spurious_notify_without_caught_up_does_not_emit() {
        // Verifies the re-check guard inside the wrapper: a stale notify
        // permit must not cause a false ready signal if the consumer is no
        // longer caught up.
        let inner = futures::stream::pending::<Result<ChangeEnvelope, cdc::StreamError>>();

        let metrics = Arc::new(KafkaMetrics::default());
        // Notify but leave has_received_stats=false (e.g. callback never ran).
        metrics.caught_up.notify_one();

        let schema = test_schema();
        let stream = inject_ready_signal_on_caught_up(inner, Arc::clone(&metrics), schema);

        let res = tokio::time::timeout(Duration::from_millis(500), Box::pin(stream).next()).await;
        assert!(
            res.is_err(),
            "spurious notify without is_caught_up must not produce a ready envelope"
        );
    }
}
