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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_shard_checkpoint(
        sequence_number: &str,
        parent_id: Option<&str>,
        position: CheckpointPosition,
    ) -> ShardCheckpoint {
        ShardCheckpoint {
            sequence_number: sequence_number.to_string(),
            parent_id: parent_id.map(std::string::ToString::to_string),
            updated_at: SystemTime::now(),
            position,
        }
    }

    mod shard_checkpoint_tests {
        use super::*;

        #[test]
        fn test_serialize_deserialize_at_position() {
            let checkpoint =
                create_shard_checkpoint("12345", Some("parent-1"), CheckpointPosition::At);

            let serialized = serde_json::to_string(&checkpoint).expect("serialize");
            let deserialized: ShardCheckpoint =
                serde_json::from_str(&serialized).expect("deserialize");

            assert_eq!(deserialized.sequence_number, "12345");
            assert_eq!(deserialized.parent_id, Some("parent-1".to_string()));
            assert_eq!(deserialized.position, CheckpointPosition::At);
        }

        #[test]
        fn test_serialize_deserialize_after_position() {
            let checkpoint = create_shard_checkpoint("67890", None, CheckpointPosition::After);

            let serialized = serde_json::to_string(&checkpoint).expect("serialize");
            let deserialized: ShardCheckpoint =
                serde_json::from_str(&serialized).expect("deserialize");

            assert_eq!(deserialized.sequence_number, "67890");
            assert_eq!(deserialized.parent_id, None);
            assert_eq!(deserialized.position, CheckpointPosition::After);
        }

        #[test]
        fn test_clone_equality() {
            let checkpoint =
                create_shard_checkpoint("seq-123", Some("parent"), CheckpointPosition::At);
            let cloned = checkpoint.clone();

            assert_eq!(checkpoint, cloned);
        }
    }

    mod checkpoint_tests {
        use super::*;

        fn create_checkpoint(shards: Vec<(&str, Option<&str>)>) -> Checkpoint {
            let shards_map = shards
                .into_iter()
                .map(|(id, parent)| {
                    (
                        id.to_string(),
                        create_shard_checkpoint("0", parent, CheckpointPosition::At),
                    )
                })
                .collect();
            Checkpoint { shards: shards_map }
        }

        #[test]
        fn test_leaf_shards_empty_checkpoint() {
            let checkpoint = Checkpoint {
                shards: HashMap::new(),
            };

            let leaves = checkpoint.leaf_shards();
            assert!(leaves.is_empty());
        }

        #[test]
        fn test_leaf_shards_single_shard_no_parent() {
            let checkpoint = create_checkpoint(vec![("shard-1", None)]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 1);
            assert!(leaves.iter().any(|(id, _)| *id == "shard-1"));
        }

        #[test]
        fn test_leaf_shards_parent_child_chain() {
            // parent -> child: only child is a leaf
            let checkpoint = create_checkpoint(vec![("parent", None), ("child", Some("parent"))]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 1);
            assert!(leaves.iter().any(|(id, _)| *id == "child"));
            assert!(!leaves.iter().any(|(id, _)| *id == "parent"));
        }

        #[test]
        fn test_leaf_shards_multiple_leaves() {
            // Two independent shards without parents
            let checkpoint = create_checkpoint(vec![("shard-1", None), ("shard-2", None)]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 2);
            assert!(leaves.iter().any(|(id, _)| *id == "shard-1"));
            assert!(leaves.iter().any(|(id, _)| *id == "shard-2"));
        }

        #[test]
        fn test_leaf_shards_split_scenario() {
            // parent splits into child-a and child-b
            let checkpoint = create_checkpoint(vec![
                ("parent", None),
                ("child-a", Some("parent")),
                ("child-b", Some("parent")),
            ]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 2);
            assert!(leaves.iter().any(|(id, _)| *id == "child-a"));
            assert!(leaves.iter().any(|(id, _)| *id == "child-b"));
            assert!(!leaves.iter().any(|(id, _)| *id == "parent"));
        }

        #[test]
        fn test_leaf_shards_three_generation_chain() {
            // grandparent -> parent -> child
            let checkpoint = create_checkpoint(vec![
                ("grandparent", None),
                ("parent", Some("grandparent")),
                ("child", Some("parent")),
            ]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 1);
            assert!(leaves.iter().any(|(id, _)| *id == "child"));
        }

        #[test]
        fn test_leaf_shards_complex_tree() {
            // Tree structure:
            //        root
            //       /    \
            //    mid1    mid2
            //     |       |
            //   leaf1   leaf2
            let checkpoint = create_checkpoint(vec![
                ("root", None),
                ("mid1", Some("root")),
                ("mid2", Some("root")),
                ("leaf1", Some("mid1")),
                ("leaf2", Some("mid2")),
            ]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 2);
            assert!(leaves.iter().any(|(id, _)| *id == "leaf1"));
            assert!(leaves.iter().any(|(id, _)| *id == "leaf2"));
        }

        #[test]
        fn test_serialize_deserialize_checkpoint() {
            let mut shards = HashMap::new();
            shards.insert(
                "shard-1".to_string(),
                create_shard_checkpoint("100", None, CheckpointPosition::After),
            );
            shards.insert(
                "shard-2".to_string(),
                create_shard_checkpoint("200", Some("shard-1"), CheckpointPosition::At),
            );

            let checkpoint = Checkpoint { shards };

            let serialized = serde_json::to_string(&checkpoint).expect("serialize");
            let deserialized: Checkpoint = serde_json::from_str(&serialized).expect("deserialize");

            assert_eq!(deserialized.shards.len(), 2);
            assert!(deserialized.shards.contains_key("shard-1"));
            assert!(deserialized.shards.contains_key("shard-2"));

            let first_shard = deserialized.shards.get("shard-1").expect("shard-1 exists");
            assert_eq!(first_shard.sequence_number, "100");
            assert_eq!(first_shard.parent_id, None);
            assert_eq!(first_shard.position, CheckpointPosition::After);

            let second_shard = deserialized.shards.get("shard-2").expect("shard-2 exists");
            assert_eq!(second_shard.sequence_number, "200");
            assert_eq!(second_shard.parent_id, Some("shard-1".to_string()));
            assert_eq!(second_shard.position, CheckpointPosition::At);
        }

        #[test]
        fn test_checkpoint_with_orphan_parent_reference() {
            // child references a parent that doesn't exist in the checkpoint
            // This is valid - parent may have been pruned
            let checkpoint = create_checkpoint(vec![("child", Some("nonexistent-parent"))]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 1);
            assert!(leaves.iter().any(|(id, _)| *id == "child"));
        }

        // BUG TEST: This test documents a potential issue with leaf_shards when
        // a parent shard exists in the checkpoint but is NOT actually a parent
        // of any shard we care about (gap in chain).
        //
        // Scenario: We have parent and child in checkpoint. Parent references
        // a grandparent that no longer exists. If we only want to resume from
        // the child (most recent), we should NOT also get the parent.
        // However, if the stream has progressed and the parent is now exhausted
        // but we still have it in the checkpoint, leaf_shards would correctly
        // NOT return the parent (since child references it).
        //
        // This test shows that leaf_shards works correctly - parent is excluded
        // because child references it as parent_id.
        #[test]
        fn test_leaf_shards_parent_excluded_when_has_child() {
            // parent-1 references a grandparent that doesn't exist
            // child-1 references parent-1
            // Only child-1 should be a leaf
            let checkpoint = create_checkpoint(vec![
                ("parent-1", Some("grandparent-missing")),
                ("child-1", Some("parent-1")),
            ]);

            let leaves = checkpoint.leaf_shards();
            assert_eq!(leaves.len(), 1);
            assert!(leaves.iter().any(|(id, _)| *id == "child-1"));
            // parent-1 should NOT be a leaf because child-1 has it as parent_id
            assert!(!leaves.iter().any(|(id, _)| *id == "parent-1"));
        }

        // BUG TEST: When checkpoint contains stale ancestor shards that should
        // have been pruned, leaf_shards might return too many shards.
        // This test documents the expected behavior.
        #[test]
        fn test_leaf_shards_with_stale_unrelated_ancestor() {
            // Scenario: Checkpoint was not properly cleaned up and contains:
            // - old-parent (no children in checkpoint, should have been pruned)
            // - new-shard (no parent in checkpoint, independent)
            // Both are returned as "leaves" even though old-parent may be exhausted
            let checkpoint = create_checkpoint(vec![
                ("old-parent", None), // This should have been pruned
                ("new-shard", None),  // Independent shard
            ]);

            let leaves = checkpoint.leaf_shards();
            // Both are leaves since neither is a parent of the other
            assert_eq!(leaves.len(), 2);
            assert!(leaves.iter().any(|(id, _)| *id == "old-parent"));
            assert!(leaves.iter().any(|(id, _)| *id == "new-shard"));
            // NOTE: This is "correct" behavior for leaf_shards, but could cause
            // problems if old-parent was already fully processed - we'd try
            // to resume from it again. The checkpoint pruning should prevent this.
        }
    }
}
