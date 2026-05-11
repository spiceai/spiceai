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

//! Streaming source implementations for benchmarks.

mod dynamodb;
pub mod debezium;
pub mod kafka;

pub use dynamodb::{DynamoDbConfig, DynamoDbStreamsSource, transform_spicepod};
pub use debezium::DebeziumIngestionSource;
pub use kafka::KafkaIngestionSource;

use test_framework::anyhow::Result;

/// Generic data-producing source for the streaming ingestion benchmark.
///
/// Implementations handle topic/table creation, row production, marker
/// insert/delete, and cleanup. The runner calls these methods in sequence.
#[async_trait::async_trait]
pub trait IngestionSource: Send + Sync {
    /// Initialize the source (create topic/table, register connector, etc.)
    async fn prepare(&mut self) -> Result<()>;

    /// Produce `count` data rows to the stream.
    async fn produce_rows(&mut self, count: u64) -> Result<()>;

    /// Produce a single sentinel marker row with the given `event_type`.
    async fn produce_marker(&mut self, marker_event_type: &str) -> Result<()>;

    /// Delete the marker row (no-op for append-only sources).
    async fn delete_marker(&mut self, marker_event_type: &str) -> Result<()>;

    /// Release all resources created by this source.
    async fn cleanup(&self) -> Result<()>;

    /// Kafka bootstrap servers string (used to configure the spicepod).
    fn kafka_bootstrap_servers(&self) -> &str;

    /// The `from` field value for the Spice dataset (e.g. `kafka:events-abc123`).
    fn source_from_field(&self) -> String;

}
