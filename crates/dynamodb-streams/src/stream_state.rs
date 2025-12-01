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
use crate::checkpoint::{CheckpointPosition, GlobalCheckpoint, ShardCheckpoint};
use crate::client_sdk::{ApiShard, SDKClient};
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug)]
pub struct DynamoDBStreamBatch {
    pub records: Vec<Record>,
    pub checkpoint: GlobalCheckpoint,
}

#[derive(Debug, PartialEq)]
pub struct ActiveShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub iterator: String,
}

impl ActiveShard {
    pub fn set_iterator(&mut self, new_iterator: String) {
        self.iterator = new_iterator;
    }
}

#[derive(Debug, PartialEq)]
pub struct PendingShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
}

#[derive(Debug)]
pub struct StreamState {
    stream_arn: String,
    active: HashMap<String, ActiveShard>,
    pending: HashMap<String, PendingShard>,
    initializing: HashMap<String, PendingShard>,
}

pub struct ShardPollResult {
    pub shard_id: String,
    pub records: Vec<Record>,
    pub checkpoint: ShardCheckpoint,
}

/// Manages shard lifecycle across three states to maintain parent-child ordering guarantees.
///
/// State Transitions:
/// - Active: Shards with iterators, ready to poll for records
/// - Pending: Shards blocked by active/initializing parents (respects lineage order)
/// - Initializing: Shards awaiting iterator initialization from AWS
///
/// Flow:
/// 1. Discovered shards enter `pending` if parent exists, otherwise `initializing`
/// 2. `initializing` → `active` when iterator obtained
/// 3. When parent exhausts, children move `pending` → `initializing`
/// 4. Failed polls can move `active` → `initializing` for fresh iterator
///
/// This ensures parent shards are fully consumed before children begin processing.
impl StreamState {
    pub fn get_active_shards(&self) -> impl Iterator<Item = &ActiveShard> {
        self.active.values()
    }

    pub fn get_initializing_shards_ids(&self) -> impl Iterator<Item = &String> {
        self.initializing.keys()
    }

    pub fn handle_poll_result(
        &mut self,
        shard_id: &str,
        new_iterator: Option<String>,
        records: Vec<Record>,
    ) -> Option<ShardPollResult> {
        tracing::debug!(
            "Processing shard poll: shard_id={:?}, new_iterator={:?}, records_num={:?}",
            shard_id,
            new_iterator,
            records.len()
        );

        let parent_id = self.active.get(shard_id)?.parent_shard_id.clone();

        if let Some(iter) = new_iterator {
            self.active.get_mut(shard_id)?.set_iterator(iter);
        } else {
            self.active.remove(shard_id);
            self.promote_children(shard_id);
        }

        if records.is_empty() {
            return None;
        }

        let last_seq_opt = records.last()?.clone().dynamodb?.sequence_number;
        tracing::debug!(
            "Shard latest sequence number: shard_id={:?}, seq_number={:?}",
            shard_id,
            last_seq_opt
        );
        if last_seq_opt.is_none() {
            tracing::error!("Missing sequence number: shard_id={}", shard_id);
        }
        let last_seq = last_seq_opt?;

        Some(ShardPollResult {
            shard_id: shard_id.to_string(),
            records,
            checkpoint: ShardCheckpoint {
                sequence_number: last_seq,
                parent_id,
                updated_at: SystemTime::now(),
                position: CheckpointPosition::After,
            },
        })
    }

    pub fn reinitialize_shard(&mut self, shard_id: &str) {
        // Remove from active
        if let Some(active_shard) = self.active.remove(shard_id) {
            // Add to initializing to get a fresh iterator
            self.initializing.insert(
                shard_id.to_string(),
                PendingShard {
                    shard_id: shard_id.to_string(),
                    parent_shard_id: active_shard.parent_shard_id,
                },
            );
        }
    }

    /// Add discovered shards, returns shard IDs that need initialization
    pub fn add_discovered(&mut self, shards: Vec<ApiShard>) {
        for shard in shards {
            let shard_id = shard.shard_id.clone();

            // At each iteration we will get all currently non-expired shards
            // Only subset of them we haven't seen before
            if self.active.contains_key(&shard_id)
                || self.pending.contains_key(&shard_id)
                || self.initializing.contains_key(&shard_id)
            {
                continue;
            }

            // Shards in DynamoDB Streams have a parent-child relationship.
            // Until we exhausted the parent shard, we don't want to read from its children.
            // As long as the parent of the discovered shard is either active/pending/initializing,
            // we add it to the pending state.
            let blocked = shard.parent_shard_id.clone().is_some_and(|p| {
                self.active.contains_key(&p)
                    || self.pending.contains_key(&p)
                    || self.initializing.contains_key(&p)
            });

            let pending_shard = PendingShard {
                shard_id: shard_id.clone(),
                parent_shard_id: shard.parent_shard_id.clone(),
            };

            if blocked {
                self.pending.insert(shard_id, pending_shard);
            } else if shard.ending_sequence_number.is_none() {
                self.initializing.insert(shard_id.clone(), pending_shard);
            }
        }
    }

    /// Move shard from initializing to active with its iterator
    pub fn mark_active(&mut self, shard_id: String, iterator: String) {
        if let Some(pending) = self.initializing.remove(&shard_id) {
            let active = ActiveShard {
                shard_id: shard_id.clone(),
                parent_shard_id: pending.parent_shard_id,
                iterator,
            };
            self.active.insert(shard_id, active);
        }
    }

    /// Promote children of exhausted parent, returns shard IDs that need initialization
    fn promote_children(&mut self, parent_id: &str) {
        let to_promote: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, s)| s.parent_shard_id.as_deref() == Some(parent_id))
            .map(|(id, _)| id.clone())
            .collect();

        for child_id in to_promote {
            if let Some(child) = self.pending.remove(&child_id) {
                self.try_move_to_initializing(&child_id, child);
            }
        }
    }

    fn try_move_to_initializing(&mut self, shard_id: &str, shard: PendingShard) {
        let is_blocked = shard.parent_shard_id.as_ref().is_some_and(|p| {
            self.active.contains_key(p)
                || self.pending.contains_key(p)
                || self.initializing.contains_key(p)
        });

        if is_blocked {
            self.pending.insert(shard_id.to_string(), shard);
        } else {
            self.initializing.insert(shard_id.to_string(), shard);
        }
    }
}

pub async fn initialize_state_from_checkpoint(
    stream_arn: String,
    checkpoint: &GlobalCheckpoint,
    sdk_client: Arc<SDKClient>,
) -> crate::Result<StreamState> {
    let mut state = StreamState {
        stream_arn: stream_arn.clone(),
        active: HashMap::new(),
        pending: HashMap::new(),
        initializing: HashMap::new(),
    };

    for (shard_id, shard_checkpoint) in checkpoint.leaf_shards() {
        let iterator_type = match shard_checkpoint.position {
            CheckpointPosition::At => ShardIteratorType::AtSequenceNumber,
            CheckpointPosition::After => ShardIteratorType::AfterSequenceNumber,
        };

        match sdk_client
            .get_shard_iterator(
                &stream_arn,
                shard_id,
                &iterator_type,
                Some(shard_checkpoint.sequence_number.clone()),
            )
            .await
        {
            Ok(Some(iterator)) => {
                let shard = ActiveShard {
                    shard_id: shard_id.to_string(),
                    parent_shard_id: shard_checkpoint.parent_id.clone(),
                    iterator,
                };

                state.active.insert(shard_id.to_string(), shard);
            }
            Ok(None) => {
                start_children_from_trim_horizon(Arc::clone(&sdk_client), &mut state, shard_id)
                    .await?;
            }
            Err(e) => {
                tracing::warn!("Failed to initialize shard {}: {}", shard_id, e);
            }
        }
    }

    Ok(state)
}

async fn start_children_from_trim_horizon(
    sdk_client: Arc<SDKClient>,
    state: &mut StreamState,
    parent_id: &str,
) -> crate::Result<()> {
    let all_shards = sdk_client.get_all_shards(&state.stream_arn).await?;

    for child in all_shards {
        if child.parent_shard_id == Some(parent_id.to_string()) {
            match sdk_client
                .get_shard_iterator(
                    &state.stream_arn,
                    &child.shard_id,
                    &ShardIteratorType::TrimHorizon,
                    None,
                )
                .await
            {
                Ok(Some(iterator)) => {
                    let shard = ActiveShard {
                        shard_id: child.shard_id.clone(),
                        parent_shard_id: Some(parent_id.to_string()),
                        iterator,
                    };
                    state.active.insert(child.shard_id.clone(), shard);
                }
                Ok(None) => {
                    tracing::debug!("Empty iterator: shard_id={}", child.shard_id);
                }
                Err(e) => {
                    tracing::warn!("Failed to initialize shard {}: {}", child.shard_id, e);
                }
            }
        }
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodbstreams::types::StreamRecord;

    impl StreamState {
        #[must_use]
        pub fn new(stream_arn: String) -> Self {
            Self {
                stream_arn,
                active: HashMap::new(),
                pending: HashMap::new(),
                initializing: HashMap::new(),
            }
        }
    }

    fn create_record(seq_num: &str) -> Record {
        Record::builder()
            .dynamodb(StreamRecord::builder().sequence_number(seq_num).build())
            .build()
    }

    fn create_record_without_seq() -> Record {
        Record::builder()
            .dynamodb(StreamRecord::builder().build())
            .build()
    }

    fn create_record_without_dynamodb() -> Record {
        Record::builder().build()
    }

    fn create_api_shard(id: &str, parent: Option<&str>, ending_seq: Option<&str>) -> ApiShard {
        ApiShard {
            shard_id: id.to_string(),
            parent_shard_id: parent.map(std::string::ToString::to_string),
            starting_sequence_number: None,
            ending_sequence_number: ending_seq.map(std::string::ToString::to_string),
        }
    }

    mod handle_poll_result {
        use super::*;

        #[test]
        fn test_handle_poll_result_shard_not_found() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let result = state.handle_poll_result(
                "nonexistent-shard",
                Some("new-iter".to_string()),
                vec![create_record("123")],
            );

            assert!(result.is_none());
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
        }

        #[test]
        fn test_handle_poll_result_updates_iterator() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "old-iter".to_string(),
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record("123")],
            );

            assert!(result.is_some());
            let batch = result.expect("Expected batch to be Some");
            assert_eq!(batch.shard_id, "shard-1");
            assert_eq!(batch.records.len(), 1);
            assert_eq!(batch.checkpoint.sequence_number, "123");
            assert_eq!(batch.checkpoint.parent_id, None);
            assert_eq!(batch.checkpoint.position, CheckpointPosition::After);

            // Check complete state
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "new-iter".to_string(),
                }
            );
        }

        #[test]
        fn test_handle_poll_result_removes_exhausted_shard() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                },
            );

            let result = state.handle_poll_result("shard-1", None, vec![create_record("123")]);

            assert!(result.is_some());
            let batch = result.expect("Expected batch to be Some");
            assert_eq!(batch.shard_id, "shard-1");
            assert_eq!(batch.records.len(), 1);
            assert_eq!(batch.checkpoint.sequence_number, "123");

            // Check complete state - shard should be removed
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(!state.active.contains_key("shard-1"));
        }

        #[test]
        fn test_handle_poll_result_empty_records_returns_none() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                },
            );

            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), vec![]);

            assert!(result.is_none());

            // Iterator should still be updated even though no records
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "new-iter".to_string(),
                }
            );
        }

        #[test]
        fn test_handle_poll_result_missing_sequence_number_returns_none() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record_without_seq()],
            );

            assert!(result.is_none());

            // State should be updated even though result is None
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "new-iter".to_string(),
                }
            );
        }

        #[test]
        fn test_handle_poll_result_missing_dynamodb_returns_none() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record_without_dynamodb()],
            );

            assert!(result.is_none());

            // State should be updated
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
        }

        #[test]
        fn test_handle_poll_result_creates_correct_batch() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent-1".to_string()),
                    iterator: "iter-1".to_string(),
                },
            );

            let records = vec![
                create_record("100"),
                create_record("101"),
                create_record("102"),
            ];

            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), records);

            assert!(result.is_some());
            let batch = result.expect("Expected batch to be Some");
            assert_eq!(batch.shard_id, "shard-1");
            assert_eq!(batch.records.len(), 3);
            assert_eq!(batch.checkpoint.sequence_number, "102");
            assert_eq!(batch.checkpoint.parent_id, Some("parent-1".to_string()));
            assert_eq!(batch.checkpoint.position, CheckpointPosition::After);

            // Verify complete state
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent-1".to_string()),
                    iterator: "new-iter".to_string(),
                }
            );
        }

        #[test]
        fn test_handle_poll_result_promotes_children_on_exhaustion() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Setup parent shard
            state.active.insert(
                "parent".to_string(),
                ActiveShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-parent".to_string(),
                },
            );

            // Setup child shard in pending
            state.pending.insert(
                "child".to_string(),
                PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );

            // Exhaust parent shard
            let result = state.handle_poll_result("parent", None, vec![create_record("100")]);

            assert!(result.is_some());
            let batch = result.expect("Expected batch to be Some");
            assert_eq!(batch.shard_id, "parent");
            assert_eq!(batch.checkpoint.sequence_number, "100");

            // Verify complete state after exhaustion
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.active.contains_key("parent"));
            assert!(!state.pending.contains_key("child"));
            assert!(state.initializing.contains_key("child"));

            assert_eq!(
                state
                    .initializing
                    .get("child")
                    .expect("child should be in initializing"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }
    }

    // ========================================
    // Tests for add_discovered()
    // ========================================
    mod add_discovered {
        use super::*;
        #[test]
        fn test_add_discovered_empty_list() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            state.add_discovered(vec![]);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.is_empty());
            assert!(state.pending.is_empty());
            assert!(state.initializing.is_empty());
        }

        #[test]
        fn test_add_discovered_open_shard_without_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let shards = vec![create_api_shard("shard-1", None, None)];

            state.add_discovered(shards);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.active.contains_key("shard-1"));
            assert!(!state.pending.contains_key("shard-1"));
            assert!(state.initializing.contains_key("shard-1"));

            assert_eq!(
                state
                    .initializing
                    .get("shard-1")
                    .expect("shard-1 should be in initializing"),
                &PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                }
            );
        }

        #[test]
        fn test_add_discovered_closed_shard_without_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let shards = vec![create_api_shard("shard-1", None, Some("999"))];

            state.add_discovered(shards);

            // Closed shards are ignored
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("shard-1"));
            assert!(!state.pending.contains_key("shard-1"));
            assert!(!state.active.contains_key("shard-1"));
        }

        #[test]
        fn test_add_discovered_child_with_active_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add parent to active
            state.active.insert(
                "parent".to_string(),
                ActiveShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    iterator: "iter".to_string(),
                },
            );

            let shards = vec![create_api_shard("child", Some("parent"), None)];
            state.add_discovered(shards);

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.active.contains_key("parent"));
            assert!(state.pending.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }

        #[test]
        fn test_add_discovered_child_with_pending_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add parent to pending
            state.pending.insert(
                "parent".to_string(),
                PendingShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                },
            );

            let shards = vec![create_api_shard("child", Some("parent"), None)];
            state.add_discovered(shards);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.pending.contains_key("parent"));
            assert!(state.pending.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }

        #[test]
        fn test_add_discovered_child_with_initializing_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add parent to initializing
            state.initializing.insert(
                "parent".to_string(),
                PendingShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                },
            );

            let shards = vec![create_api_shard("child", Some("parent"), None)];
            state.add_discovered(shards);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 1);

            assert!(state.initializing.contains_key("parent"));
            assert!(state.pending.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }

        #[test]
        fn test_add_discovered_ignores_existing_active_shard() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "original-iter".to_string(),
                },
            );

            let shards = vec![create_api_shard("shard-1", None, None)];
            state.add_discovered(shards);

            // Should not change existing active shard
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.active.contains_key("shard-1"));
            assert!(!state.pending.contains_key("shard-1"));
            assert!(!state.initializing.contains_key("shard-1"));

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "original-iter".to_string(),
                }
            );
        }

        #[test]
        fn test_add_discovered_ignores_existing_pending_shard() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.pending.insert(
                "shard-1".to_string(),
                PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );

            let shards = vec![create_api_shard("shard-1", None, None)];
            state.add_discovered(shards);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.active.contains_key("shard-1"));
            assert!(state.pending.contains_key("shard-1"));
            assert!(!state.initializing.contains_key("shard-1"));

            assert_eq!(
                state
                    .pending
                    .get("shard-1")
                    .expect("shard-1 should be in pending"),
                &PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                }
            );
        }

        #[test]
        fn test_add_discovered_ignores_existing_initializing_shard() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );

            let shards = vec![create_api_shard("shard-1", None, None)];
            state.add_discovered(shards);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.active.contains_key("shard-1"));
            assert!(!state.pending.contains_key("shard-1"));
            assert!(state.initializing.contains_key("shard-1"));

            assert_eq!(
                state
                    .initializing
                    .get("shard-1")
                    .expect("shard-1 should be in initializing"),
                &PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                }
            );
        }

        #[test]
        fn test_add_discovered_multiple_shards_mixed_states() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "parent-1".to_string(),
                ActiveShard {
                    shard_id: "parent-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter".to_string(),
                },
            );

            let shards = vec![
                create_api_shard("root-1", None, None), // Should go to initializing
                create_api_shard("root-2", None, Some("999")), // Should be ignored (closed)
                create_api_shard("child-1", Some("parent-1"), None), // Should go to pending
                create_api_shard("child-2", Some("nonexistent"), None), // Should go to initializing
            ];

            state.add_discovered(shards);

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 2);

            // root-1: open shard without parent
            assert!(state.initializing.contains_key("root-1"));
            assert_eq!(
                state
                    .initializing
                    .get("root-1")
                    .expect("root-1 should be in initializing"),
                &PendingShard {
                    shard_id: "root-1".to_string(),
                    parent_shard_id: None,
                }
            );

            // root-2: closed shard, should not exist anywhere
            assert!(!state.active.contains_key("root-2"));
            assert!(!state.pending.contains_key("root-2"));
            assert!(!state.initializing.contains_key("root-2"));

            // child-1: blocked by active parent
            assert!(state.pending.contains_key("child-1"));
            assert_eq!(
                state
                    .pending
                    .get("child-1")
                    .expect("child-1 should be in pending"),
                &PendingShard {
                    shard_id: "child-1".to_string(),
                    parent_shard_id: Some("parent-1".to_string()),
                }
            );

            // child-2: parent doesn't exist, not blocked
            assert!(state.initializing.contains_key("child-2"));
            assert_eq!(
                state
                    .initializing
                    .get("child-2")
                    .expect("child-2 should be in initializing"),
                &PendingShard {
                    shard_id: "child-2".to_string(),
                    parent_shard_id: Some("nonexistent".to_string()),
                }
            );

            // parent-1 should still be active
            assert!(state.active.contains_key("parent-1"));
        }
    }

    mod mark_active {
        use super::*;

        #[test]
        fn test_mark_active_moves_from_initializing_to_active() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );

            state.mark_active("shard-1".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("shard-1"));
            assert!(!state.pending.contains_key("shard-1"));
            assert!(state.active.contains_key("shard-1"));

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "iterator-1".to_string(),
                }
            );
        }

        #[test]
        fn test_mark_active_without_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );

            state.mark_active("shard-1".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.active.contains_key("shard-1"));
            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iterator-1".to_string(),
                }
            );
        }

        #[test]
        fn test_mark_active_nonexistent_shard_does_nothing() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.mark_active("nonexistent".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.active.contains_key("nonexistent"));
            assert!(!state.pending.contains_key("nonexistent"));
            assert!(!state.initializing.contains_key("nonexistent"));
        }

        #[test]
        fn test_mark_active_from_pending_does_nothing() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.pending.insert(
                "shard-1".to_string(),
                PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );

            state.mark_active("shard-1".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.active.contains_key("shard-1"));
            assert!(state.pending.contains_key("shard-1"));
            assert!(!state.initializing.contains_key("shard-1"));

            assert_eq!(
                state
                    .pending
                    .get("shard-1")
                    .expect("shard-1 should be in pending"),
                &PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                }
            );
        }

        #[test]
        fn test_mark_active_multiple_shards() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );
            state.initializing.insert(
                "shard-2".to_string(),
                PendingShard {
                    shard_id: "shard-2".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );

            state.mark_active("shard-1".to_string(), "iter-1".to_string());
            state.mark_active("shard-2".to_string(), "iter-2".to_string());

            assert_eq!(state.active.len(), 2);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                }
            );

            assert_eq!(
                state
                    .active
                    .get("shard-2")
                    .expect("shard-2 should be in active"),
                &ActiveShard {
                    shard_id: "shard-2".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "iter-2".to_string(),
                }
            );
        }
    }

    mod promote_children {
        use super::*;

        #[test]
        fn test_promote_children_single_child() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.pending.insert(
                "child".to_string(),
                PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );

            state.promote_children("parent");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.pending.contains_key("child"));
            assert!(state.initializing.contains_key("child"));

            assert_eq!(
                state
                    .initializing
                    .get("child")
                    .expect("child should be in initializing"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }

        #[test]
        fn test_promote_children_multiple_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.pending.insert(
                "child-1".to_string(),
                PendingShard {
                    shard_id: "child-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );
            state.pending.insert(
                "child-2".to_string(),
                PendingShard {
                    shard_id: "child-2".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );
            state.pending.insert(
                "other-child".to_string(),
                PendingShard {
                    shard_id: "other-child".to_string(),
                    parent_shard_id: Some("other-parent".to_string()),
                },
            );

            state.promote_children("parent");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 2);

            assert!(!state.pending.contains_key("child-1"));
            assert!(!state.pending.contains_key("child-2"));
            assert!(state.initializing.contains_key("child-1"));
            assert!(state.initializing.contains_key("child-2"));

            // Other child should remain pending
            assert!(state.pending.contains_key("other-child"));

            assert_eq!(
                state
                    .initializing
                    .get("child-1")
                    .expect("child-1 should be in initializing"),
                &PendingShard {
                    shard_id: "child-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );

            assert_eq!(
                state
                    .initializing
                    .get("child-2")
                    .expect("child-2 should be in initializing"),
                &PendingShard {
                    shard_id: "child-2".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );

            assert_eq!(
                state
                    .pending
                    .get("other-child")
                    .expect("other-child should be in pending"),
                &PendingShard {
                    shard_id: "other-child".to_string(),
                    parent_shard_id: Some("other-parent".to_string()),
                }
            );
        }

        #[test]
        fn test_promote_children_no_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.pending.insert(
                "unrelated".to_string(),
                PendingShard {
                    shard_id: "unrelated".to_string(),
                    parent_shard_id: Some("other-parent".to_string()),
                },
            );

            state.promote_children("parent");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.pending.contains_key("unrelated"));
            assert!(!state.initializing.contains_key("unrelated"));

            assert_eq!(
                state
                    .pending
                    .get("unrelated")
                    .expect("unrelated should be in pending"),
                &PendingShard {
                    shard_id: "unrelated".to_string(),
                    parent_shard_id: Some("other-parent".to_string()),
                }
            );
        }

        #[test]
        fn test_promote_children_with_grandparent_blocking() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Grandparent is still active
            state.active.insert(
                "grandparent".to_string(),
                ActiveShard {
                    shard_id: "grandparent".to_string(),
                    parent_shard_id: None,
                    iterator: "iter".to_string(),
                },
            );

            // Child pending on grandparent (not on parent that we're promoting from)
            state.pending.insert(
                "child".to_string(),
                PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("grandparent".to_string()),
                },
            );

            // Try to promote children of "parent" - but child belongs to grandparent
            state.promote_children("parent");

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            // Child should remain pending because it doesn't belong to "parent"
            assert!(state.pending.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));
            assert!(state.active.contains_key("grandparent"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("grandparent".to_string()),
                }
            );
        }
    }

    mod try_move_to_initializing {
        use super::*;

        #[test]
        fn test_try_move_to_initializing_not_blocked() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            let shard = PendingShard {
                shard_id: "shard-1".to_string(),
                parent_shard_id: None,
            };

            state.try_move_to_initializing("shard-1", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(state.initializing.contains_key("shard-1"));
            assert!(!state.pending.contains_key("shard-1"));
            assert!(!state.active.contains_key("shard-1"));

            assert_eq!(
                state
                    .initializing
                    .get("shard-1")
                    .expect("shard-1 should be in initializing"),
                &PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                }
            );
        }

        #[test]
        fn test_try_move_to_initializing_blocked_by_active_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "parent".to_string(),
                ActiveShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    iterator: "iter".to_string(),
                },
            );

            let shard = PendingShard {
                shard_id: "child".to_string(),
                parent_shard_id: Some("parent".to_string()),
            };

            state.try_move_to_initializing("child", shard);

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("child"));
            assert!(state.pending.contains_key("child"));
            assert!(state.active.contains_key("parent"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }

        #[test]
        fn test_try_move_to_initializing_blocked_by_pending_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.pending.insert(
                "parent".to_string(),
                PendingShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                },
            );

            let shard = PendingShard {
                shard_id: "child".to_string(),
                parent_shard_id: Some("parent".to_string()),
            };

            state.try_move_to_initializing("child", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("child"));
            assert!(state.pending.contains_key("child"));
            assert!(state.pending.contains_key("parent"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );
        }

        #[test]
        fn test_try_move_to_initializing_blocked_by_initializing_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "parent".to_string(),
                PendingShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                },
            );

            let shard = PendingShard {
                shard_id: "child".to_string(),
                parent_shard_id: Some("parent".to_string()),
            };

            state.try_move_to_initializing("child", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.initializing.contains_key("child"));
            assert!(state.pending.contains_key("child"));
            assert!(state.initializing.contains_key("parent"));

            assert_eq!(
                state
                    .pending
                    .get("child")
                    .expect("child should be in pending"),
                &PendingShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                }
            );

            assert_eq!(
                state
                    .initializing
                    .get("parent")
                    .expect("parent should be in initializing"),
                &PendingShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                }
            );
        }
    }

    mod complex_scenarios {
        use super::*;

        #[test]
        fn test_integration_full_shard_lifecycle() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Discover initial shard
            state.add_discovered(vec![create_api_shard("shard-1", None, None)]);
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("shard-1"));

            // Mark it active
            state.mark_active("shard-1".to_string(), "iter-1".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("shard-1"));

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                }
            );

            // Poll with records
            let batch = state.handle_poll_result(
                "shard-1",
                Some("iter-2".to_string()),
                vec![create_record("100")],
            );
            assert!(batch.is_some());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-2".to_string(),
                }
            );

            // Exhaust shard
            let batch = state.handle_poll_result("shard-1", None, vec![create_record("101")]);
            assert!(batch.is_some());
            let batch = batch.expect("Expected batch to be Some");
            assert_eq!(batch.shard_id, "shard-1");
            assert_eq!(batch.checkpoint.sequence_number, "101");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(!state.active.contains_key("shard-1"));
        }

        #[test]
        fn test_integration_parent_child_promotion() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Discover parent and child
            state.add_discovered(vec![
                create_api_shard("parent", None, None),
                create_api_shard("child", Some("parent"), None),
            ]);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("parent"));
            assert!(state.pending.contains_key("child"));

            // Activate parent
            state.mark_active("parent".to_string(), "parent-iter".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("parent"));
            assert!(state.pending.contains_key("child"));

            assert_eq!(
                state
                    .active
                    .get("parent")
                    .expect("parent should be in active"),
                &ActiveShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    iterator: "parent-iter".to_string(),
                }
            );

            // Exhaust parent
            let batch = state.handle_poll_result("parent", None, vec![create_record("100")]);
            assert!(batch.is_some());

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(!state.active.contains_key("parent"));
            assert!(!state.pending.contains_key("child"));
            assert!(state.initializing.contains_key("child"));

            // Activate child
            state.mark_active("child".to_string(), "child-iter".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("child"));

            assert_eq!(
                state
                    .active
                    .get("child")
                    .expect("child should be in active"),
                &ActiveShard {
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "child-iter".to_string(),
                }
            );
        }

        #[test]
        fn test_integration_multiple_generations() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add three generations
            state.add_discovered(vec![
                create_api_shard("gen1", None, None),
                create_api_shard("gen2", Some("gen1"), None),
                create_api_shard("gen3", Some("gen2"), None),
            ]);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("gen1"));
            assert!(state.pending.contains_key("gen2"));
            assert!(state.pending.contains_key("gen3"));

            // Activate gen1
            state.mark_active("gen1".to_string(), "iter1".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            // Exhaust gen1
            state.handle_poll_result("gen1", None, vec![create_record("100")]);
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("gen2"));
            assert!(state.pending.contains_key("gen3"));

            // Activate gen2
            state.mark_active("gen2".to_string(), "iter2".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            // Exhaust gen2
            state.handle_poll_result("gen2", None, vec![create_record("200")]);
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("gen3"));

            // Activate gen3
            state.mark_active("gen3".to_string(), "iter3".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("gen3"));

            assert_eq!(
                state.active.get("gen3").expect("gen3 should be in active"),
                &ActiveShard {
                    shard_id: "gen3".to_string(),
                    parent_shard_id: Some("gen2".to_string()),
                    iterator: "iter3".to_string(),
                }
            );
        }

        #[test]
        fn test_integration_shard_split_two_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Parent splits into two children
            state.add_discovered(vec![
                create_api_shard("parent", None, None),
                create_api_shard("child-a", Some("parent"), None),
                create_api_shard("child-b", Some("parent"), None),
            ]);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 1);

            state.mark_active("parent".to_string(), "parent-iter".to_string());

            // Both children should be pending
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.pending.contains_key("child-a"));
            assert!(state.pending.contains_key("child-b"));

            // Exhaust parent
            state.handle_poll_result("parent", None, vec![create_record("100")]);

            // Both children should now be initializing
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 2);
            assert!(state.initializing.contains_key("child-a"));
            assert!(state.initializing.contains_key("child-b"));

            // Activate both
            state.mark_active("child-a".to_string(), "iter-a".to_string());
            state.mark_active("child-b".to_string(), "iter-b".to_string());

            assert_eq!(state.active.len(), 2);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("child-a"));
            assert!(state.active.contains_key("child-b"));

            assert_eq!(
                state
                    .active
                    .get("child-a")
                    .expect("child-a should be in active"),
                &ActiveShard {
                    shard_id: "child-a".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "iter-a".to_string(),
                }
            );

            assert_eq!(
                state
                    .active
                    .get("child-b")
                    .expect("child-b should be in active"),
                &ActiveShard {
                    shard_id: "child-b".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "iter-b".to_string(),
                }
            );
        }

        #[test]
        fn test_integration_rediscovery_of_existing_shards() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add initial shard
            state.add_discovered(vec![create_api_shard("shard-1", None, None)]);
            state.mark_active("shard-1".to_string(), "iter-1".to_string());

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            // Rediscover same shard - should be ignored
            state.add_discovered(vec![create_api_shard("shard-1", None, None)]);

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                }
            );
        }

        #[test]
        fn test_integration_handle_poll_with_multiple_records() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "iter-1".to_string(),
                },
            );

            let records = vec![
                create_record("100"),
                create_record("101"),
                create_record("102"),
                create_record("103"),
                create_record("104"),
            ];

            let batch = state.handle_poll_result("shard-1", Some("iter-2".to_string()), records);

            assert!(batch.is_some());
            let batch = batch.expect("Expected batch to be Some");
            assert_eq!(batch.shard_id, "shard-1");
            assert_eq!(batch.records.len(), 5);
            assert_eq!(batch.checkpoint.sequence_number, "104");
            assert_eq!(batch.checkpoint.parent_id, Some("parent".to_string()));
            assert_eq!(batch.checkpoint.position, CheckpointPosition::After);

            // Verify state
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert_eq!(
                state
                    .active
                    .get("shard-1")
                    .expect("shard-1 should be in active"),
                &ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    iterator: "iter-2".to_string(),
                }
            );
        }

        #[test]
        fn test_edge_case_empty_parent_id() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            let shard = PendingShard {
                shard_id: "shard-1".to_string(),
                parent_shard_id: None,
            };

            state.try_move_to_initializing("shard-1", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("shard-1"));

            assert_eq!(
                state
                    .initializing
                    .get("shard-1")
                    .expect("shard-1 should be in initializing"),
                &PendingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                }
            );
        }

        #[test]
        fn test_edge_case_promote_with_no_pending_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "parent".to_string(),
                ActiveShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    iterator: "iter".to_string(),
                },
            );

            // Should not panic
            state.promote_children("parent");

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 0);
            assert_eq!(state.initializing.len(), 0);
        }

        #[test]
        fn test_concurrent_children_from_different_parents() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Two independent parent-child chains
            state.add_discovered(vec![
                create_api_shard("parent-1", None, None),
                create_api_shard("parent-2", None, None),
                create_api_shard("child-1", Some("parent-1"), None),
                create_api_shard("child-2", Some("parent-2"), None),
            ]);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 2);

            state.mark_active("parent-1".to_string(), "iter1".to_string());
            state.mark_active("parent-2".to_string(), "iter2".to_string());

            assert_eq!(state.active.len(), 2);
            assert_eq!(state.pending.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            // Exhaust parent-1
            state.handle_poll_result("parent-1", None, vec![create_record("100")]);

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.initializing.len(), 1);

            assert!(state.initializing.contains_key("child-1"));
            assert!(state.pending.contains_key("child-2"));
            assert!(state.active.contains_key("parent-2"));
            assert!(!state.active.contains_key("parent-1"));

            assert_eq!(
                state
                    .initializing
                    .get("child-1")
                    .expect("child-1 should be in initializing"),
                &PendingShard {
                    shard_id: "child-1".to_string(),
                    parent_shard_id: Some("parent-1".to_string()),
                }
            );

            assert_eq!(
                state
                    .pending
                    .get("child-2")
                    .expect("child-2 should be in pending"),
                &PendingShard {
                    shard_id: "child-2".to_string(),
                    parent_shard_id: Some("parent-2".to_string()),
                }
            );

            assert_eq!(
                state
                    .active
                    .get("parent-2")
                    .expect("parent-2 should be in active"),
                &ActiveShard {
                    shard_id: "parent-2".to_string(),
                    parent_shard_id: None,
                    iterator: "iter2".to_string(),
                }
            );
        }
    }
}
