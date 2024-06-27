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
#![allow(clippy::module_name_repetitions)]

use futures::{Stream, StreamExt};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    util::get_rdkafka_version,
    ClientConfig, Message,
};
use serde::de::DeserializeOwned;
use snafu::prelude::*;

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct KafkaConsumer {
    group_id: String,
    consumer: StreamConsumer,
}

impl KafkaConsumer {
    pub fn create_with_existing_group_id(
        group_id: impl Into<String>,
        brokers: String,
    ) -> Result<Self> {
        Self::create(group_id.into(), brokers)
    }

    pub fn create_with_generated_group_id(dataset: &str, brokers: String) -> Result<Self> {
        Self::create(Self::generate_group_id(dataset), brokers)
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
    pub async fn next_json<T: DeserializeOwned>(&self) -> Result<Option<KafkaMessage<T>>> {
        let mut stream = Box::pin(self.stream_json::<T>());
        stream.next().await.transpose()
    }

    /// Stream JSON messages from the Kafka topic.
    pub fn stream_json<T: DeserializeOwned>(&self) -> impl Stream<Item = Result<KafkaMessage<T>>> {
        self.consumer.stream().filter_map(move |msg| async move {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => return Some(Err(Error::UnableToReceiveMessage { source: e })),
            };

            let payload = msg.payload()?;

            let value = match serde_json::from_slice(payload) {
                Ok(value) => value,
                Err(e) => return Some(Err(Error::UnableToDeserializeJsonMessage { source: e })),
            };

            Some(Ok(KafkaMessage::new(&self.consumer, msg, value)))
        })
    }

    fn create(group_id: String, brokers: String) -> Result<Self> {
        let (_, version) = get_rdkafka_version();
        tracing::debug!("rd_kafka_version: {}", version);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id.clone())
            .set("bootstrap.servers", brokers)
            // For new consumer groups, start reading at the beginning of the topic
            .set("auto.offset.reset", "smallest")
            // Commit offsets automatically
            .set("enable.auto.commit", "true")
            // Commit offsets every 5 seconds
            .set("auto.commit.interval.ms", "5000")
            // Don't automatically store offsets the library provides to us - we will store them after processing explicitly
            // This is what gives us the "at least once" semantics
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .context(UnableToCreateConsumerSnafu)?;

        Ok(Self { group_id, consumer })
    }

    fn generate_group_id(dataset: &str) -> String {
        format!("spice.ai-{dataset}-{}", uuid::Uuid::new_v4())
    }
}

pub struct KafkaMessage<'a, T> {
    consumer: &'a StreamConsumer,
    msg: BorrowedMessage<'a>,
    pub value: T,
}

impl<'a, T> KafkaMessage<'a, T> {
    fn new(consumer: &'a StreamConsumer, msg: BorrowedMessage<'a>, value: T) -> Self {
        Self {
            consumer,
            msg,
            value,
        }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn mark_processed(&self) -> Result<()> {
        self.consumer
            .store_offset_from_message(&self.msg)
            .context(UnableToCommitMessageSnafu)
    }
}
