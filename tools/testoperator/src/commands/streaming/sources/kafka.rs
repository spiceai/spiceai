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

//! Kafka append-mode ingestion source for streaming benchmarks.
//!
//! Creates (or recreates) a named topic with infinite retention, produces
//! JSON-encoded events and a permanent `benchmark-ready` marker.
//! The topic persists across runs; each benchmark run replays from offset 0
//! using a unique consumer group.

use std::time::Duration;

use futures::future::try_join_all;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use test_framework::anyhow::{self, Context, Result};

use super::IngestionSource;

// Single partition ensures ordered delivery so the marker is always last.
const TOPIC_PARTITIONS: i32 = 1;
pub const MARKER_EVENT_TYPE: &str = "benchmark-ready";

/// Configuration for the Kafka ingestion source.
///
/// Environment variables:
/// - `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: localhost:9092)
/// - `KAFKA_SECURITY_PROTOCOL`: Security protocol (default: PLAINTEXT)
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub security_protocol: String,
}

impl KafkaConfig {
    pub fn from_env() -> Self {
        Self {
            bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            security_protocol: std::env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "PLAINTEXT".to_string()),
        }
    }
}

pub struct KafkaIngestionSource {
    config: KafkaConfig,
    topic: String,
    batch_size: usize,
    producer: Option<FutureProducer>,
    next_id: u64,
}

impl KafkaIngestionSource {
    pub fn new(config: KafkaConfig, topic: String, batch_size: usize) -> Self {
        Self { config, topic, batch_size, producer: None, next_id: 1 }
    }

    fn make_client_config(&self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("security.protocol", &self.config.security_protocol);
        cfg
    }

    fn make_admin(&self) -> Result<AdminClient<DefaultClientContext>> {
        self.make_client_config()
            .create()
            .context("Failed to create Kafka admin client")
    }

    fn make_event_payload(&self, id: u64, event_type: &str) -> Vec<u8> {
        const EVENT_TYPES: &[&str] = &["click", "view", "purchase", "refund", "signup"];
        let user_id = ((id % 10_000) + 1) as i64;
        let et = if event_type == "generated" {
            EVENT_TYPES[(id as usize) % EVENT_TYPES.len()]
        } else {
            event_type
        };
        let amount = (id as f64 * 0.01) % 1000.0;
        let payload = format!("payload-{id}");
        serde_json::json!({
            "id": id as i64,
            "user_id": user_id,
            "event_type": et,
            "payload": payload,
            "amount": amount,
        })
        .to_string()
        .into_bytes()
    }
}

#[async_trait::async_trait]
impl IngestionSource for KafkaIngestionSource {
    async fn prepare(&mut self) -> Result<()> {
        println!(
            "Preparing Kafka ingestion source (bootstrap: {}, topic: {})",
            self.config.bootstrap_servers, self.topic
        );

        let admin = self.make_admin()?;

        // Delete and recreate to ensure a clean slate with correct config.
        let _ = admin
            .delete_topics(&[self.topic.as_str()], &AdminOptions::new())
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Single partition for guaranteed ordering; infinite retention.
        let new_topic = NewTopic::new(&self.topic, TOPIC_PARTITIONS, TopicReplication::Fixed(1))
            .set("retention.ms", "-1");
        admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await
            .context("Failed to create Kafka topic")?;

        tokio::time::sleep(Duration::from_secs(2)).await;
        println!("Created topic '{}' (partitions=1, retention=infinite)", self.topic);

        let producer: FutureProducer = self
            .make_client_config()
            .set("message.timeout.ms", "30000")
            .set("batch.num.messages", "10000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("queue.buffering.max.ms", "5")
            .create()
            .context("Failed to create Kafka producer")?;
        self.producer = Some(producer);

        Ok(())
    }

    async fn produce_rows(&mut self, count: u64) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Producer not initialized — call prepare() first"))?;

        let topic = self.topic.clone();
        let batch_size = self.batch_size;
        let mut produced = 0u64;

        for chunk_start in (0..count).step_by(batch_size) {
            let chunk_len = (count - chunk_start).min(batch_size as u64) as usize;
            let base_id = self.next_id;
            self.next_id += chunk_len as u64;

            // Collect payloads before creating futures so they outlive the borrows.
            let events: Vec<(Vec<u8>, String)> = (0..chunk_len)
                .map(|i| {
                    let id = base_id + i as u64;
                    let payload = self.make_event_payload(id, "generated");
                    let key = id.to_string();
                    (payload, key)
                })
                .collect();

            // Send all messages in the batch, then await all delivery reports concurrently.
            let futures: Vec<_> = events
                .iter()
                .map(|(payload, key)| {
                    producer.send(
                        FutureRecord::to(&topic)
                            .payload(payload.as_slice())
                            .key(key.as_bytes()),
                        Duration::from_secs(30),
                    )
                })
                .collect();

            try_join_all(futures)
                .await
                .map_err(|(err, _)| anyhow::anyhow!("Kafka send error: {err}"))?;

            produced += chunk_len as u64;
            if produced % 50_000 == 0 || produced == count {
                println!("  Produced {produced}/{count} rows to '{topic}'");
            }
        }

        Ok(())
    }

    async fn produce_marker(&mut self, marker_event_type: &str) -> Result<()> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Producer not initialized"))?;

        let id = self.next_id;
        self.next_id += 1;

        let payload = serde_json::json!({
            "id": id as i64,
            "user_id": -1_i64,
            "event_type": marker_event_type,
            "payload": "marker",
            "amount": 0.0_f64,
        })
        .to_string()
        .into_bytes();

        let key = id.to_string();
        producer
            .send(
                FutureRecord::to(&self.topic)
                    .payload(&payload)
                    .key(key.as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| anyhow::anyhow!("Kafka marker send error: {err}"))?;

        println!("Produced marker to '{}' (event_type={marker_event_type})", self.topic);
        Ok(())
    }

    async fn delete_marker(&mut self, _marker_event_type: &str) -> Result<()> {
        // Kafka append mode — markers are permanent.
        Ok(())
    }

    /// No-op: the topic persists so future benchmark runs can replay from offset 0.
    async fn cleanup(&self) -> Result<()> {
        println!(
            "Kafka topic '{}' retained for future benchmark runs.",
            self.topic
        );
        Ok(())
    }

    fn kafka_bootstrap_servers(&self) -> &str {
        &self.config.bootstrap_servers
    }

    fn source_from_field(&self) -> String {
        format!("kafka:{}", self.topic)
    }
}
