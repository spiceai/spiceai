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

//! The **`MongoDB`** checkpoint shape: the change-stream resume token a dataset's
//! acceleration is complete as of.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::CheckpointError;

/// A dataset's change-stream resume position.
///
/// `schema_json` is the schema in its durable JSON encoding rather than an Arrow
/// `SchemaRef`, which is what keeps this crate free of an Arrow dependency. Callers
/// convert with `arrow_tools::schema::{schema_to_json, schema_from_json}`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MongoCheckpointMetadata {
    /// Canonical extended JSON of the most recent resume token's raw BSON.
    pub resume_token_json: String,
    /// Cluster operation time (seconds since epoch), used as the
    /// `startAtOperationTime` fallback when the resume token is past the oplog window.
    #[serde(default)]
    pub cluster_time_ts: Option<i64>,
    /// Serialized schema snapshot, for detecting drift between runs.
    #[serde(default)]
    pub schema_json: Option<String>,
    /// When the row was last written, as recorded by the store.
    #[serde(default)]
    pub updated_at: Option<SystemTime>,
}

/// The `MongoDB` checkpoint store, satisfied by the accelerator and called by the
/// `MongoDB` data connector. Object-safe, so it is used as
/// `Arc<dyn MongoCheckpointStore>`.
#[async_trait::async_trait]
pub trait MongoCheckpointStore: Send + Sync {
    /// Load this dataset's resume position, or `None` when there is nothing to resume
    /// from.
    ///
    /// A failed read is reported as `None`, not as an error, for the same reason as the
    /// `MySQL` binlog store: the connector's only recovery from an unreadable token is
    /// the same as from an absent one.
    async fn get(&self) -> Option<MongoCheckpointMetadata>;

    /// Persist a resume position, overwriting any previous one.
    async fn upsert(&self, metadata: &MongoCheckpointMetadata) -> Result<(), CheckpointError>;

    /// Discard this dataset's resume position, so the next run re-bootstraps.
    async fn delete(&self) -> Result<(), CheckpointError>;
}
