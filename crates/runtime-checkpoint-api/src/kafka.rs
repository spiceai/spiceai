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

//! The **Kafka** checkpoint shape: one metadata row per dataset plus one offset row
//! per `(topic, partition)`.
//!
//! Offsets are deliberately *rows*, not a field of the metadata blob. A Kafka dataset
//! advances many partitions independently and must resolve a concurrent write to the
//! higher offset; squashing them into one value would both serialize unrelated
//! partitions onto a single row and lose that per-partition merge.

use serde::{Deserialize, Serialize};

use crate::CheckpointError;

/// The committed offset of one `(topic, partition)`, as persisted in the
/// per-partition offset table.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KafkaOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl KafkaOffset {
    /// The offset to resume reading *from*, i.e. one past the committed one.
    ///
    /// Saturating: an `i64::MAX` offset cannot be advanced past, and wrapping to a
    /// negative offset would restart the partition from its beginning.
    #[must_use]
    pub fn next_read_offset(&self) -> i64 {
        self.offset.saturating_add(1)
    }
}

/// The per-dataset Kafka checkpoint: which consumer group and topic the accelerated
/// data belongs to, the schema it was read with, and the committed offsets.
///
/// `schema_json` is the schema in its durable JSON encoding rather than an Arrow
/// `SchemaRef`, which is what keeps this crate free of an Arrow dependency. Callers
/// convert with `arrow_tools::schema::{schema_to_json, schema_from_json}`.
#[derive(Clone, Debug)]
pub struct KafkaCheckpoint {
    pub consumer_group_id: String,
    pub topic: String,
    pub schema_json: String,
    pub offsets: Vec<KafkaOffset>,
}

/// The Kafka checkpoint store, satisfied by the accelerator and called by the Kafka
/// data connector. Object-safe, so it is used as `Arc<dyn KafkaCheckpointStore>`.
#[async_trait::async_trait]
pub trait KafkaCheckpointStore: Send + Sync {
    /// Load this dataset's checkpoint, or `Ok(None)` when none has been persisted.
    ///
    /// An `Err` is a failure to *read*, which the connector must not confuse with
    /// "no checkpoint": the latter restarts the topic from the beginning.
    async fn get(&self) -> Result<Option<KafkaCheckpoint>, CheckpointError>;

    /// Persist the whole checkpoint — metadata row and offset rows together.
    async fn upsert(&self, checkpoint: &KafkaCheckpoint) -> Result<(), CheckpointError>;

    /// Advance only the offset rows, leaving the metadata row untouched.
    ///
    /// Each row resolves to the **greater** of the stored and the incoming offset, so
    /// a late or out-of-order commit can never move a partition backwards. This is the
    /// hot path: it runs on every refresh commit, whereas [`Self::upsert`] runs once
    /// at bootstrap.
    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError>;
}
