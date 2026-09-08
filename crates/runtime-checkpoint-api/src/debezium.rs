/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The **Debezium** checkpoint shape: one metadata row per dataset plus one offset row
//! per `(topic, partition)`.
//!
//! Debezium reads its change events from Kafka, so its offsets are literally Kafka
//! offsets and reuse [`crate::kafka::KafkaOffset`] — the per-partition table is the same
//! shape, for the same reason (many partitions advance independently and a concurrent
//! write must resolve to the higher offset).
//!
//! The metadata row differs from Kafka's: it records the source table's primary keys and
//! the Debezium change-event field descriptors rather than an Arrow schema.

use crate::{CheckpointError, kafka::KafkaOffset};

/// The per-dataset Debezium checkpoint.
///
/// `schema_fields_json` is the connector's serialized change-event field list. It
/// travels as JSON for the same reason a Kafka schema does: the descriptor type lives
/// above this crate, and the store only round-trips the string.
#[derive(Clone, Debug)]
pub struct DebeziumCheckpoint {
    pub consumer_group_id: String,
    pub topic: String,
    /// Primary key columns of the source table, used to route updates and deletes.
    pub primary_keys: Vec<String>,
    pub schema_fields_json: String,
    pub offsets: Vec<KafkaOffset>,
}

/// The Debezium checkpoint store, satisfied by the accelerator and called by the
/// Debezium data connector. Object-safe, so it is used as
/// `Arc<dyn DebeziumCheckpointStore>`.
#[async_trait::async_trait]
pub trait DebeziumCheckpointStore: Send + Sync {
    /// Load this dataset's checkpoint, or `Ok(None)` when none has been persisted.
    ///
    /// An `Err` is a failure to *read*, which the connector must not confuse with "no
    /// checkpoint": the latter restarts the topic from the beginning.
    async fn get(&self) -> Result<Option<DebeziumCheckpoint>, CheckpointError>;

    /// Persist the whole checkpoint — metadata row and offset rows together.
    async fn upsert(&self, checkpoint: &DebeziumCheckpoint) -> Result<(), CheckpointError>;

    /// Advance only the offset rows, leaving the metadata row untouched.
    ///
    /// Each row resolves to the **greater** of the stored and the incoming offset, so a
    /// late or out-of-order commit can never move a partition backwards.
    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError>;
}
