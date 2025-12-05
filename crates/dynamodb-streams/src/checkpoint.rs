/*
Copyright 2025 The Spice.ai OSS Authors

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
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

/// Checkpoint representing progress within a single shard.
///
/// Stores the position (sequence number) and whether to resume
/// at (inclusive) or after (exclusive) that position.
///
/// Inclusive (`At`) checkpoint `Client.latest_global_checkpoint()`
/// since no records have been processed yet.
///
/// Exclusive (`After`) checkpoint is returned as part of `StreamResult`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardCheckpoint {
    pub sequence_number: String,
    pub parent_id: Option<String>, // Root shards don't have parents
    pub updated_at: SystemTime,
    pub position: CheckpointPosition,
}

/// Determines whether to resume processing at or after a sequence number.
///
/// - `At`: Resume starting AT this sequence (inclusive) - record not yet processed
/// - `After`: Resume starting AFTER this sequence (exclusive) - record already processed
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CheckpointPosition {
    At,
    After,
}

/// Snapshot of processing progress across all shards.
///
/// Captures the state of multiple shards at a point in time, allowing
/// resumption from this position. Can be persisted and used to restart
/// streaming within the 24-hour `DynamoDB` Streams retention window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub shards: HashMap<String, ShardCheckpoint>,
}

impl Checkpoint {
    /// Returns shards that have no children in this checkpoint (leaf nodes in the lineage tree).
    /// These are the active shards to resume from, as their parents are already exhausted.
    #[must_use]
    pub fn leaf_shards(&self) -> Vec<(&String, &ShardCheckpoint)> {
        let parent_ids: HashSet<&str> = self
            .shards
            .values()
            .filter_map(|sc| sc.parent_id.as_deref())
            .collect();

        self.shards
            .iter()
            .filter(|(shard_id, _)| !parent_ids.contains(shard_id.as_str()))
            .collect()
    }
}
