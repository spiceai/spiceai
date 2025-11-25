use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

#[derive(Clone, Debug)]
pub struct ShardCheckpoint {
    pub sequence_number: String,
    pub parent_id: Option<String>, // Root shards don't have parents
    pub updated_at: SystemTime,
    pub position: CheckpointPosition,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CheckpointPosition {
    At,
    After,
}

#[derive(Clone, Debug)]
pub struct GlobalCheckpoint {
    pub shards: HashMap<String, ShardCheckpoint>,
}

impl GlobalCheckpoint {
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
