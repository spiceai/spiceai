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
use crate::checkpoint::{Checkpoint, CheckpointPosition, ShardCheckpoint};
use crate::client_sdk::{ApiShard, SDKClient};
use crate::{Error, MissingStaringSequenceNumberSnafu, Result};
use aws_sdk_dynamodbstreams::primitives::DateTime;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use snafu::OptionExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq, Clone)]
pub struct ActiveShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub iterator: String,
    pub last_checkpoint: ShardCheckpoint,
    pub last_produced_at: Option<SystemTime>,
    pub current_watermark: Option<SystemTime>,
}

impl ActiveShard {
    pub fn update_iterator(&mut self, new_iterator: String) {
        self.iterator = new_iterator;
    }

    pub fn update_checkpoint(&mut self, checkpoint: ShardCheckpoint) {
        self.last_checkpoint = checkpoint;
    }

    pub fn update_produced_at(&mut self) {
        self.last_produced_at = Some(SystemTime::now());
    }

    pub fn update_watermark(&mut self, watermark: SystemTime) {
        self.current_watermark = Some(watermark);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct InitializingShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub last_checkpoint: ShardCheckpoint,
    pub last_produced_at: Option<SystemTime>,
    pub current_watermark: Option<SystemTime>,
}

#[derive(Debug, PartialEq)]
pub struct BlockedShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub last_checkpoint: ShardCheckpoint,
}

#[derive(Debug)]
pub struct StreamState {
    // Shards ready to be polled on the next iteration so they have `iterator` field.
    // These shards participate in checkpoint so they have `last_checkpoint` field.
    active: HashMap<String, ActiveShard>,
    // Shards with checkpoint, but no iterator.
    // It could be either because their iterator expired, or they went through checkpoint initialization process
    initializing: HashMap<String, InitializingShard>,
    // Shards that are blocked by their parent
    blocked: HashMap<String, BlockedShard>,
}

pub struct ShardPollResult {
    pub shard_id: String,
    pub outcome: PollOutcome,
    pub last_checkpoint: ShardCheckpoint,
    pub last_produced_at: Option<SystemTime>,
    pub current_watermark: Option<SystemTime>,
}

pub enum PollOutcome {
    Records { records: Vec<Record> },
    Empty,
    Failed,
}

impl PollOutcome {
    pub fn is_empty(&self) -> bool {
        matches!(self, PollOutcome::Empty)
    }
}

impl StreamState {
    pub fn get_active_shards(&self) -> impl Iterator<Item = &ActiveShard> {
        self.active.values()
    }

    pub fn get_initializing_shards(&self) -> impl Iterator<Item = &InitializingShard> {
        self.initializing.values()
    }

    pub fn handle_poll_result(
        &mut self,
        shard_id: &str,
        new_iterator: Option<String>,
        records: Vec<Record>,
    ) -> Result<ShardPollResult> {
        let Some(shard) = self.active.get(shard_id) else {
            return Err(Error::UnexpectedShardId {
                shard_id: shard_id.to_string(),
            });
        };
        let mut current_checkpoint = shard.last_checkpoint.clone();
        let mut current_last_produced_at = shard.last_produced_at;
        let mut current_watermark = shard.current_watermark;

        // First update watermark and checkpoint if possible
        if !records.is_empty()
            && let Some(shard) = self.active.get_mut(shard_id)
        {
            shard.update_produced_at();
            current_last_produced_at = shard.last_produced_at;

            // Update watermark
            let max_event_time = records
                .iter()
                .filter_map(|r| r.dynamodb.as_ref()?.approximate_creation_date_time)
                .max()
                .map(datetime_to_system_time);

            if let Some(event_time) = max_event_time {
                shard.update_watermark(event_time);
                current_watermark = shard.current_watermark;
            }

            // Update checkpoint
            let sequence_number = records
                .last()
                .and_then(|r| r.dynamodb.as_ref())
                .and_then(|db| db.sequence_number.clone());

            if let Some(seq) = sequence_number {
                let checkpoint = ShardCheckpoint {
                    sequence_number: seq,
                    parent_id: shard.parent_shard_id.clone(),
                    updated_at: SystemTime::now(),
                    position: CheckpointPosition::After,
                };
                shard.update_checkpoint(checkpoint.clone());
                current_checkpoint = checkpoint;
            } else {
                tracing::warn!(
                    "Missing sequence number for shard {}, keeping previous checkpoint",
                    shard_id
                );
            }
        }

        // Check if shard is exhausted and can be removed
        if let Some(iter) = new_iterator {
            if let Some(shard) = self.active.get_mut(shard_id) {
                shard.update_iterator(iter);
            }
        } else {
            self.active.remove(shard_id);
            self.promote_children(shard_id);
        }

        let outcome = if records.is_empty() {
            PollOutcome::Empty
        } else {
            PollOutcome::Records { records }
        };

        Ok(ShardPollResult {
            shard_id: shard_id.to_string(),
            outcome,
            last_checkpoint: current_checkpoint,
            last_produced_at: current_last_produced_at,
            current_watermark,
        })
    }

    pub fn handle_poll_error(&mut self, shard_id: &str, error: Error) -> Result<ShardPollResult> {
        let Some(shard) = self.active.get(shard_id) else {
            return Err(Error::UnexpectedShardId {
                shard_id: shard_id.to_string(),
            });
        };

        // Capture current state before any modifications
        let result = ShardPollResult {
            shard_id: shard_id.to_string(),
            outcome: PollOutcome::Failed,
            last_checkpoint: shard.last_checkpoint.clone(),
            last_produced_at: shard.last_produced_at,
            current_watermark: shard.current_watermark,
        };

        // Handle iterator expiration by reinitializing with current checkpoint
        if error.is_retriable() {
            tracing::warn!(
                "Poll error for shard {}. Will retry on next iteration: {}",
                shard_id,
                error
            );
        } else if matches!(error, Error::IteratorExpired) {
            tracing::warn!(
                "Iterator expired for shard {}, marking for reinitialization with checkpoint: {:?}",
                shard_id,
                shard.last_checkpoint
            );
            self.reinitialize_shard_with_checkpoint(shard_id);
        } else {
            return Err(error);
        }

        Ok(result)
    }

    /// Reinitialize a shard when its iterator expires.
    /// The shard will be moved to initializing state and will get a new iterator
    /// based on its last checkpoint position.
    pub fn reinitialize_shard_with_checkpoint(&mut self, shard_id: &str) {
        if let Some(active_shard) = self.active.remove(shard_id) {
            // Move to initializing to get a fresh iterator
            self.initializing.insert(
                shard_id.to_string(),
                InitializingShard {
                    shard_id: shard_id.to_string(),
                    parent_shard_id: active_shard.parent_shard_id,
                    last_checkpoint: active_shard.last_checkpoint,
                    last_produced_at: active_shard.last_produced_at,
                    current_watermark: active_shard.current_watermark,
                },
            );
        }
    }

    /// Add discovered shards, returns shard IDs that need initialization
    pub fn add_discovered(&mut self, shards: Vec<ApiShard>) -> Result<()> {
        for shard in shards {
            let shard_id = shard.shard_id.clone();

            // At each iteration we will get all currently non-expired shards
            // Only subset of them we haven't seen before
            if self.active.contains_key(&shard_id)
                || self.blocked.contains_key(&shard_id)
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
                    || self.blocked.contains_key(&p)
                    || self.initializing.contains_key(&p)
            });

            tracing::debug!(
                "Discovered new shard: id={}, parent={:?}, blocked={}",
                shard_id,
                shard.parent_shard_id,
                blocked
            );

            let checkpoint = ShardCheckpoint {
                sequence_number: shard
                    .starting_sequence_number
                    .context(MissingStaringSequenceNumberSnafu)?,
                parent_id: shard.parent_shard_id.clone(),
                updated_at: SystemTime::now(),
                position: CheckpointPosition::At,
            };

            if blocked {
                self.blocked.insert(
                    shard_id.clone(),
                    BlockedShard {
                        shard_id: shard_id.clone(),
                        parent_shard_id: shard.parent_shard_id.clone(),
                        last_checkpoint: checkpoint,
                    },
                );
            } else {
                self.initializing.insert(
                    shard_id.clone(),
                    InitializingShard {
                        shard_id: shard_id.clone(),
                        parent_shard_id: shard.parent_shard_id.clone(),
                        last_checkpoint: checkpoint,
                        last_produced_at: None,
                        current_watermark: None,
                    },
                );
            }
        }

        Ok(())
    }

    /// Move shard from initializing to active with its iterator
    pub fn mark_active(&mut self, shard_id: String, iterator: String) {
        if let Some(pending) = self.initializing.remove(&shard_id) {
            let active = ActiveShard {
                shard_id: shard_id.clone(),
                parent_shard_id: pending.parent_shard_id,
                last_checkpoint: pending.last_checkpoint,
                last_produced_at: pending.last_produced_at,
                current_watermark: pending.current_watermark,
                iterator,
            };
            self.active.insert(shard_id, active);
        }
    }

    fn promote_children(&mut self, parent_id: &str) {
        let to_promote: Vec<String> = self
            .blocked
            .iter()
            .filter(|(_, s)| s.parent_shard_id.as_deref() == Some(parent_id))
            .map(|(id, _)| id.clone())
            .collect();

        for child_id in to_promote {
            if let Some(child) = self.blocked.remove(&child_id) {
                self.try_move_to_initializing(&child_id, child);
            }
        }
    }

    fn try_move_to_initializing(&mut self, shard_id: &str, shard: BlockedShard) {
        let is_blocked = shard.parent_shard_id.as_ref().is_some_and(|p| {
            self.active.contains_key(p)
                || self.blocked.contains_key(p)
                || self.initializing.contains_key(p)
        });

        if is_blocked {
            self.blocked.insert(shard_id.to_string(), shard);
        } else {
            self.initializing.insert(
                shard_id.to_string(),
                InitializingShard {
                    shard_id: shard_id.to_string(),
                    parent_shard_id: shard.parent_shard_id,
                    last_checkpoint: shard.last_checkpoint,
                    last_produced_at: None,
                    current_watermark: None,
                },
            );
        }
    }
}

pub async fn initialize_state_from_checkpoint(
    stream_arn: String,
    checkpoint: &Checkpoint,
    sdk_client: Arc<SDKClient>,
) -> Result<StreamState> {
    let mut state = StreamState {
        active: HashMap::new(),
        blocked: HashMap::new(),
        initializing: HashMap::new(),
    };

    for (shard_id, shard_checkpoint) in checkpoint.leaf_shards() {
        let iterator_type = match shard_checkpoint.position {
            CheckpointPosition::At => ShardIteratorType::AtSequenceNumber,
            CheckpointPosition::After => ShardIteratorType::AfterSequenceNumber,
        };

        let iterator = sdk_client
            .get_shard_iterator(
                &stream_arn,
                shard_id,
                &iterator_type,
                Some(shard_checkpoint.sequence_number.clone()),
            )
            .await?;

        tracing::debug!(
            "Initialized shard from checkpoint: id={}, parent={:?}",
            shard_id,
            shard_checkpoint.parent_id
        );

        let shard = ActiveShard {
            shard_id: shard_id.to_string(),
            parent_shard_id: shard_checkpoint.parent_id.clone(),
            iterator,
            last_checkpoint: shard_checkpoint.clone(),
            last_produced_at: None,
            current_watermark: None,
        };

        state.active.insert(shard_id.to_string(), shard);
    }

    Ok(state)
}

pub fn datetime_to_system_time(dt: DateTime) -> SystemTime {
    let secs = dt.secs();
    let subsec_nanos = dt.subsec_nanos();
    UNIX_EPOCH + Duration::new(secs.try_into().unwrap_or(0), subsec_nanos)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use aws_sdk_dynamodbstreams::types::StreamRecord;

    impl StreamState {
        #[must_use]
        pub fn new(_stream_arn: String) -> Self {
            Self {
                active: HashMap::new(),
                blocked: HashMap::new(),
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
            starting_sequence_number: Some("0".to_string()),
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

            assert!(result.is_err());
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record("123")],
            );

            assert!(result.is_ok());
            let result = result.expect("Expected result to be Ok");
            assert_eq!(result.shard_id, "shard-1");
            if let PollOutcome::Records { records } = result.outcome {
                assert_eq!(records.len(), 1);
            } else {
                panic!("Expected PollOutcome::Records");
            }
            assert_eq!(result.last_checkpoint.sequence_number, "123");
            assert_eq!(result.last_checkpoint.parent_id, None);
            assert_eq!(result.last_checkpoint.position, CheckpointPosition::After);

            // Check complete state
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "new-iter");
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let result = state.handle_poll_result("shard-1", None, vec![create_record("123")]);

            assert!(result.is_ok());
            let result = result.expect("Expected result to be Ok");
            assert_eq!(result.shard_id, "shard-1");
            if let PollOutcome::Records { records } = result.outcome {
                assert_eq!(records.len(), 1);
            } else {
                panic!("Expected PollOutcome::Records");
            }
            assert_eq!(result.last_checkpoint.sequence_number, "123");

            // Check complete state - shard should be removed
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), vec![]);

            assert!(result.is_ok());
            if let Ok(result) = result {
                assert!(matches!(result.outcome, PollOutcome::Empty));
            }

            // Iterator should still be updated even though no records
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "new-iter");
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record_without_seq()],
            );

            result.expect("result");
            // When sequence number is missing, the record is still returned
            // but the checkpoint is not updated

            // State should be updated
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "new-iter");
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record_without_dynamodb()],
            );

            result.expect("result");
            // When dynamodb field is missing, the record is still returned
            // but the checkpoint is not updated

            // State should be updated
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: Some("parent-1".to_string()),
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let records = vec![
                create_record("100"),
                create_record("101"),
                create_record("102"),
            ];

            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), records);

            assert!(result.is_ok());
            let result = result.expect("Expected result to be Ok");
            assert_eq!(result.shard_id, "shard-1");
            if let PollOutcome::Records { records } = result.outcome {
                assert_eq!(records.len(), 3);
            } else {
                panic!("Expected PollOutcome::Records");
            }
            assert_eq!(result.last_checkpoint.sequence_number, "102");
            assert_eq!(
                result.last_checkpoint.parent_id,
                Some("parent-1".to_string())
            );
            assert_eq!(result.last_checkpoint.position, CheckpointPosition::After);

            // Verify complete state
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, Some("parent-1".to_string()));
            assert_eq!(active_shard.iterator, "new-iter");
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            // Setup child shard in pending
            state.blocked.insert(
                "child".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );

            // Exhaust parent shard
            let result = state.handle_poll_result("parent", None, vec![create_record("100")]);

            assert!(result.is_ok());
            let result = result.expect("Expected result to be Ok");
            assert_eq!(result.shard_id, "parent");
            assert_eq!(result.last_checkpoint.sequence_number, "100");

            // Verify complete state after exhaustion
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.active.contains_key("parent"));
            assert!(!state.blocked.contains_key("child"));
            assert!(state.initializing.contains_key("child"));

            let child_shard = state
                .initializing
                .get("child")
                .expect("child should be in initializing");
            assert_eq!(child_shard.shard_id, "child");
            assert_eq!(child_shard.parent_shard_id, Some("parent".to_string()));
        }

        #[test]
        fn test_handle_poll_result_updates_watermark() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: "99".to_string(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let records = vec![
                create_record_with_timestamp("100", DateTime::from_secs(1000)),
                create_record_with_timestamp("101", DateTime::from_secs(1010)), // Latest
                create_record_with_timestamp("102", DateTime::from_secs(1005)),
            ];

            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), records);

            assert!(result.is_ok());
            let result = result.expect("result");

            // Verify watermark is max timestamp (1010)
            assert!(result.current_watermark.is_some());
            let expected_watermark = datetime_to_system_time(DateTime::from_secs(1010));
            assert_eq!(result.current_watermark.unwrap(), expected_watermark);

            // Verify last_produced_at is set
            assert!(result.last_produced_at.is_some());

            // Verify active shard state
            let shard = state.active.get("shard-1").unwrap();
            assert_eq!(shard.current_watermark.unwrap(), expected_watermark);
            assert!(shard.last_produced_at.is_some());
            assert_eq!(shard.last_checkpoint.sequence_number, "102");
        }

        #[test]
        fn test_handle_poll_result_watermark_not_updated_without_timestamps() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: "99".to_string(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            // Records without approximate_creation_date_time
            let records = vec![create_record("100")]; // Assuming this creates without timestamp

            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), records);

            assert!(result.is_ok());
            let result = result.expect("result");

            // Watermark should still be None
            assert!(result.current_watermark.is_none());

            // But last_produced_at should be set
            assert!(result.last_produced_at.is_some());

            // Checkpoint should be updated
            assert_eq!(result.last_checkpoint.sequence_number, "100");

            // Verify active shard
            let shard = state.active.get("shard-1").unwrap();
            assert!(shard.current_watermark.is_none());
            assert!(shard.last_produced_at.is_some());
        }

        #[test]
        fn test_handle_poll_result_missing_sequence_keeps_old_checkpoint() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let old_checkpoint_time = SystemTime::now();

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: "99".to_string(),
                        parent_id: None,
                        updated_at: old_checkpoint_time,
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let result = state.handle_poll_result(
                "shard-1",
                Some("new-iter".to_string()),
                vec![create_record_without_seq()],
            );

            assert!(result.is_ok());
            let result = result.expect("result");

            // Checkpoint sequence should be unchanged
            assert_eq!(result.last_checkpoint.sequence_number, "99");

            // But records should still be returned
            if let PollOutcome::Records { records } = result.outcome {
                assert_eq!(records.len(), 1);
            } else {
                panic!("Expected PollOutcome::Records");
            }

            // Verify active shard kept old checkpoint
            let shard = state.active.get("shard-1").unwrap();
            assert_eq!(shard.last_checkpoint.sequence_number, "99");
        }

        #[test]
        fn test_handle_poll_result_exhausted_with_watermark() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let base_time = DateTime::from_secs(2000);

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: "99".to_string(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let records = vec![create_record_with_timestamp("100", base_time)];

            // Shard exhausted (new_iterator = None) but has final records
            let result = state.handle_poll_result("shard-1", None, records);

            assert!(result.is_ok());
            let result = result.expect("result");

            // Should have records
            if let PollOutcome::Records { records } = result.outcome {
                assert_eq!(records.len(), 1);
            } else {
                panic!("Expected PollOutcome::Records");
            }

            // Should have updated checkpoint
            assert_eq!(result.last_checkpoint.sequence_number, "100");

            // Should have watermark
            assert!(result.current_watermark.is_some());
            assert_eq!(
                result.current_watermark.unwrap(),
                datetime_to_system_time(base_time)
            );

            // Should have last_produced_at
            assert!(result.last_produced_at.is_some());

            // Shard should be removed
            assert!(!state.active.contains_key("shard-1"));
        }

        #[test]
        fn test_handle_poll_result_exhausted_empty_records() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let old_watermark = datetime_to_system_time(DateTime::from_secs(1000));
            let old_produced_at = SystemTime::now();

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    iterator: "iter-1".to_string(),
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: "99".to_string(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: Some(old_produced_at),
                    current_watermark: Some(old_watermark),
                },
            );

            // Shard exhausted with no records
            let result = state.handle_poll_result("shard-1", None, vec![]);

            assert!(result.is_ok());
            let result = result.expect("result");

            // Should be empty
            assert!(matches!(result.outcome, PollOutcome::Empty));

            // Should preserve old checkpoint, watermark, last_produced_at
            assert_eq!(result.last_checkpoint.sequence_number, "99");
            assert_eq!(result.current_watermark.unwrap(), old_watermark);
            assert_eq!(result.last_produced_at.unwrap(), old_produced_at);

            // Shard should be removed
            assert!(!state.active.contains_key("shard-1"));
        }

        #[test]
        fn test_handle_poll_result_verifies_all_shard_fields() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let base_time = DateTime::from_secs(3000);

            state.active.insert(
                "shard-1".to_string(),
                ActiveShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent-1".to_string()),
                    iterator: "old-iter".to_string(),
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: "50".to_string(),
                        parent_id: Some("parent-1".to_string()),
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let records = vec![create_record_with_timestamp("100", base_time)];
            let result = state.handle_poll_result("shard-1", Some("new-iter".to_string()), records);

            result.expect("result");

            // Verify active shard has ALL fields updated correctly
            let shard = state.active.get("shard-1").expect("shard should exist");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, Some("parent-1".to_string()));
            assert_eq!(shard.iterator, "new-iter");
            assert_eq!(shard.last_checkpoint.sequence_number, "100");
            assert_eq!(
                shard.last_checkpoint.parent_id,
                Some("parent-1".to_string())
            );
            assert_eq!(shard.last_checkpoint.position, CheckpointPosition::After);
            assert!(shard.last_produced_at.is_some());
            assert_eq!(
                shard.current_watermark.unwrap(),
                datetime_to_system_time(base_time)
            );
        }

        // Helper function to create record with timestamp
        fn create_record_with_timestamp(seq: &str, timestamp: DateTime) -> Record {
            Record::builder()
                .dynamodb(
                    StreamRecord::builder()
                        .sequence_number(seq.to_string())
                        .approximate_creation_date_time(timestamp)
                        .build(),
                )
                .build()
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
            state.add_discovered(vec![]).unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.is_empty());
            assert!(state.blocked.is_empty());
            assert!(state.initializing.is_empty());
        }

        #[test]
        fn test_add_discovered_open_shard_without_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let shards = vec![create_api_shard("shard-1", None, None)];

            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.active.contains_key("shard-1"));
            assert!(!state.blocked.contains_key("shard-1"));
            assert!(state.initializing.contains_key("shard-1"));

            let shard = state
                .initializing
                .get("shard-1")
                .expect("shard-1 should be in initializing");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
        }

        #[test]
        fn test_add_discovered_closed_shard_without_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());
            let shards = vec![create_api_shard("shard-1", None, Some("999"))];

            state.add_discovered(shards).unwrap();

            // Closed shards are ignored
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            let shard = state
                .initializing
                .get("shard-1")
                .expect("shard-1 should be in initializing");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
            assert_eq!(shard.last_checkpoint.sequence_number, "0");
            assert_eq!(shard.last_checkpoint.parent_id, None);
            assert_eq!(shard.last_checkpoint.position, CheckpointPosition::At);
            assert!(!state.blocked.contains_key("shard-1"));
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shards = vec![create_api_shard("child", Some("parent"), None)];
            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.active.contains_key("parent"));
            assert!(state.blocked.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));
            assert_eq!(child.last_checkpoint.sequence_number, "0");
            assert_eq!(child.last_checkpoint.parent_id, Some("parent".to_string()));
            assert_eq!(child.last_checkpoint.position, CheckpointPosition::At);
        }

        #[test]
        fn test_add_discovered_child_with_pending_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add parent to pending
            state.blocked.insert(
                "parent".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                },
            );

            let shards = vec![create_api_shard("child", Some("parent"), None)];
            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.blocked.contains_key("parent"));
            assert!(state.blocked.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));
            assert_eq!(child.last_checkpoint.sequence_number, "0");
            assert_eq!(child.last_checkpoint.parent_id, Some("parent".to_string()));
            assert_eq!(child.last_checkpoint.position, CheckpointPosition::At);
        }

        #[test]
        fn test_add_discovered_child_with_initializing_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add parent to initializing
            state.initializing.insert(
                "parent".to_string(),
                InitializingShard {
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shards = vec![create_api_shard("child", Some("parent"), None)];
            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 1);

            assert!(state.initializing.contains_key("parent"));
            assert!(state.blocked.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));
            assert_eq!(child.last_checkpoint.sequence_number, "0");
            assert_eq!(child.last_checkpoint.parent_id, Some("parent".to_string()));
            assert_eq!(child.last_checkpoint.position, CheckpointPosition::At);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shards = vec![create_api_shard("shard-1", None, None)];
            state.add_discovered(shards).unwrap();

            // Should not change existing active shard
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.active.contains_key("shard-1"));
            assert!(!state.blocked.contains_key("shard-1"));
            assert!(!state.initializing.contains_key("shard-1"));

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "original-iter");
        }

        #[test]
        fn test_add_discovered_ignores_existing_pending_shard() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.blocked.insert(
                "shard-1".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );

            let shards = vec![create_api_shard("shard-1", None, None)];
            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.active.contains_key("shard-1"));
            assert!(state.blocked.contains_key("shard-1"));
            assert!(!state.initializing.contains_key("shard-1"));

            let shard = state
                .blocked
                .get("shard-1")
                .expect("shard-1 should be in pending");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
        }

        #[test]
        fn test_add_discovered_ignores_existing_initializing_shard() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                InitializingShard {
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shards = vec![create_api_shard("shard-1", None, None)];
            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.active.contains_key("shard-1"));
            assert!(!state.blocked.contains_key("shard-1"));
            assert!(state.initializing.contains_key("shard-1"));

            let shard = state
                .initializing
                .get("shard-1")
                .expect("shard-1 should be in initializing");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shards = vec![
                create_api_shard("root-1", None, None), // Should go to initializing
                create_api_shard("root-2", None, Some("999")), // Shouldgo to initializing
                create_api_shard("child-1", Some("parent-1"), None), // Should go to pending
                create_api_shard("child-2", Some("nonexistent"), None), // Should go to initializing
            ];

            state.add_discovered(shards).unwrap();

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 3);

            // root-1: open shard without parent
            assert!(state.initializing.contains_key("root-1"));
            let root1 = state
                .initializing
                .get("root-1")
                .expect("root-1 should be in initializing");
            assert_eq!(root1.shard_id, "root-1");
            assert_eq!(root1.parent_shard_id, None);

            // root-2: open shard without parent
            assert!(state.initializing.contains_key("root-2"));
            let root2 = state
                .initializing
                .get("root-2")
                .expect("root-2 should be in initializing");
            assert_eq!(root2.shard_id, "root-2");
            assert_eq!(root2.parent_shard_id, None);

            // child-1: blocked by active parent
            assert!(state.blocked.contains_key("child-1"));
            let child1 = state
                .blocked
                .get("child-1")
                .expect("child-1 should be in pending");
            assert_eq!(child1.shard_id, "child-1");
            assert_eq!(child1.parent_shard_id, Some("parent-1".to_string()));
            assert_eq!(child1.last_checkpoint.sequence_number, "0");
            assert_eq!(
                child1.last_checkpoint.parent_id,
                Some("parent-1".to_string())
            );
            assert_eq!(child1.last_checkpoint.position, CheckpointPosition::At);

            // child-2: parent doesn't exist, not blocked
            assert!(state.initializing.contains_key("child-2"));
            let child2 = state
                .initializing
                .get("child-2")
                .expect("child-2 should be in initializing");
            assert_eq!(child2.shard_id, "child-2");
            assert_eq!(child2.parent_shard_id, Some("nonexistent".to_string()));

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
                InitializingShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            state.mark_active("shard-1".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("shard-1"));
            assert!(!state.blocked.contains_key("shard-1"));
            assert!(state.active.contains_key("shard-1"));

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, Some("parent".to_string()));
            assert_eq!(active_shard.iterator, "iterator-1");
        }

        #[test]
        fn test_mark_active_without_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                InitializingShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            state.mark_active("shard-1".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.active.contains_key("shard-1"));
            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "iterator-1");
        }

        #[test]
        fn test_mark_active_nonexistent_shard_does_nothing() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.mark_active("nonexistent".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.active.contains_key("nonexistent"));
            assert!(!state.blocked.contains_key("nonexistent"));
            assert!(!state.initializing.contains_key("nonexistent"));
        }

        #[test]
        fn test_mark_active_from_pending_does_nothing() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.blocked.insert(
                "shard-1".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                },
            );

            state.mark_active("shard-1".to_string(), "iterator-1".to_string());

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.active.contains_key("shard-1"));
            assert!(state.blocked.contains_key("shard-1"));
            assert!(!state.initializing.contains_key("shard-1"));

            let shard = state
                .blocked
                .get("shard-1")
                .expect("shard-1 should be in pending");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
        }

        #[test]
        fn test_mark_active_multiple_shards() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "shard-1".to_string(),
                InitializingShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "shard-1".to_string(),
                    parent_shard_id: None,
                    last_produced_at: None,
                    current_watermark: None,
                },
            );
            state.initializing.insert(
                "shard-2".to_string(),
                InitializingShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "shard-2".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            state.mark_active("shard-1".to_string(), "iter-1".to_string());
            state.mark_active("shard-2".to_string(), "iter-2".to_string());

            assert_eq!(state.active.len(), 2);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let shard1 = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(shard1.shard_id, "shard-1");
            assert_eq!(shard1.parent_shard_id, None);
            assert_eq!(shard1.iterator, "iter-1");

            let shard2 = state
                .active
                .get("shard-2")
                .expect("shard-2 should be in active");
            assert_eq!(shard2.shard_id, "shard-2");
            assert_eq!(shard2.parent_shard_id, Some("parent".to_string()));
            assert_eq!(shard2.iterator, "iter-2");
        }
    }

    mod promote_children {
        use super::*;

        #[test]
        fn test_promote_children_single_child() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.blocked.insert(
                "child".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );

            state.promote_children("parent");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.blocked.contains_key("child"));
            assert!(state.initializing.contains_key("child"));

            let child = state
                .initializing
                .get("child")
                .expect("child should be in initializing");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));
        }

        #[test]
        fn test_promote_children_multiple_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.blocked.insert(
                "child-1".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "child-1".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );
            state.blocked.insert(
                "child-2".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "child-2".to_string(),
                    parent_shard_id: Some("parent".to_string()),
                },
            );
            state.blocked.insert(
                "other-child".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "other-child".to_string(),
                    parent_shard_id: Some("other-parent".to_string()),
                },
            );

            state.promote_children("parent");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 2);

            assert!(!state.blocked.contains_key("child-1"));
            assert!(!state.blocked.contains_key("child-2"));
            assert!(state.initializing.contains_key("child-1"));
            assert!(state.initializing.contains_key("child-2"));

            // Other child should remain pending
            assert!(state.blocked.contains_key("other-child"));

            let child1 = state
                .initializing
                .get("child-1")
                .expect("child-1 should be in initializing");
            assert_eq!(child1.shard_id, "child-1");
            assert_eq!(child1.parent_shard_id, Some("parent".to_string()));

            let child2 = state
                .initializing
                .get("child-2")
                .expect("child-2 should be in initializing");
            assert_eq!(child2.shard_id, "child-2");
            assert_eq!(child2.parent_shard_id, Some("parent".to_string()));

            let other_child = state
                .blocked
                .get("other-child")
                .expect("other-child should be in pending");
            assert_eq!(other_child.shard_id, "other-child");
            assert_eq!(
                other_child.parent_shard_id,
                Some("other-parent".to_string())
            );
        }

        #[test]
        fn test_promote_children_no_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.blocked.insert(
                "unrelated".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "unrelated".to_string(),
                    parent_shard_id: Some("other-parent".to_string()),
                },
            );

            state.promote_children("parent");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(state.blocked.contains_key("unrelated"));
            assert!(!state.initializing.contains_key("unrelated"));

            let unrelated = state
                .blocked
                .get("unrelated")
                .expect("unrelated should be in pending");
            assert_eq!(unrelated.shard_id, "unrelated");
            assert_eq!(unrelated.parent_shard_id, Some("other-parent".to_string()));
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            // Child pending on grandparent (not on parent that we're promoting from)
            state.blocked.insert(
                "child".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "child".to_string(),
                    parent_shard_id: Some("grandparent".to_string()),
                },
            );

            // Try to promote children of "parent" - but child belongs to grandparent
            state.promote_children("parent");

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            // Child should remain pending because it doesn't belong to "parent"
            assert!(state.blocked.contains_key("child"));
            assert!(!state.initializing.contains_key("child"));
            assert!(state.active.contains_key("grandparent"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("grandparent".to_string()));
        }
    }

    mod try_move_to_initializing {
        use super::*;

        #[test]
        fn test_try_move_to_initializing_not_blocked() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            let shard = BlockedShard {
                last_checkpoint: ShardCheckpoint {
                    sequence_number: String::new(),
                    parent_id: None,
                    updated_at: SystemTime::now(),
                    position: CheckpointPosition::After,
                },
                shard_id: "shard-1".to_string(),
                parent_shard_id: None,
            };

            state.try_move_to_initializing("shard-1", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);

            assert!(state.initializing.contains_key("shard-1"));
            assert!(!state.blocked.contains_key("shard-1"));
            assert!(!state.active.contains_key("shard-1"));

            let shard = state
                .initializing
                .get("shard-1")
                .expect("shard-1 should be in initializing");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shard = BlockedShard {
                last_checkpoint: ShardCheckpoint {
                    sequence_number: String::new(),
                    parent_id: None,
                    updated_at: SystemTime::now(),
                    position: CheckpointPosition::After,
                },
                shard_id: "child".to_string(),
                parent_shard_id: Some("parent".to_string()),
            };

            state.try_move_to_initializing("child", shard);

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("child"));
            assert!(state.blocked.contains_key("child"));
            assert!(state.active.contains_key("parent"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));
        }

        #[test]
        fn test_try_move_to_initializing_blocked_by_pending_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.blocked.insert(
                "parent".to_string(),
                BlockedShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                },
            );

            let shard = BlockedShard {
                last_checkpoint: ShardCheckpoint {
                    sequence_number: String::new(),
                    parent_id: None,
                    updated_at: SystemTime::now(),
                    position: CheckpointPosition::After,
                },
                shard_id: "child".to_string(),
                parent_shard_id: Some("parent".to_string()),
            };

            state.try_move_to_initializing("child", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            assert!(!state.initializing.contains_key("child"));
            assert!(state.blocked.contains_key("child"));
            assert!(state.blocked.contains_key("parent"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));
        }

        #[test]
        fn test_try_move_to_initializing_blocked_by_initializing_parent() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            state.initializing.insert(
                "parent".to_string(),
                InitializingShard {
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    shard_id: "parent".to_string(),
                    parent_shard_id: None,
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let shard = BlockedShard {
                last_checkpoint: ShardCheckpoint {
                    sequence_number: String::new(),
                    parent_id: None,
                    updated_at: SystemTime::now(),
                    position: CheckpointPosition::After,
                },
                shard_id: "child".to_string(),
                parent_shard_id: Some("parent".to_string()),
            };

            state.try_move_to_initializing("child", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 1);

            assert!(!state.initializing.contains_key("child"));
            assert!(state.blocked.contains_key("child"));
            assert!(state.initializing.contains_key("parent"));

            let child = state
                .blocked
                .get("child")
                .expect("child should be in pending");
            assert_eq!(child.shard_id, "child");
            assert_eq!(child.parent_shard_id, Some("parent".to_string()));

            let parent = state
                .initializing
                .get("parent")
                .expect("parent should be in initializing");
            assert_eq!(parent.shard_id, "parent");
            assert_eq!(parent.parent_shard_id, None);
        }
    }

    mod complex_scenarios {
        use super::*;

        #[test]
        fn test_integration_full_shard_lifecycle() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Discover initial shard
            state
                .add_discovered(vec![create_api_shard("shard-1", None, None)])
                .unwrap();
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("shard-1"));

            // Mark it active
            state.mark_active("shard-1".to_string(), "iter-1".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("shard-1"));

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "iter-1");

            // Poll with records
            let batch = state.handle_poll_result(
                "shard-1",
                Some("iter-2".to_string()),
                vec![create_record("100")],
            );
            batch.unwrap();
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "iter-2");

            // Exhaust shard
            let result = state.handle_poll_result("shard-1", None, vec![create_record("101")]);
            assert!(result.is_ok());
            let result = result.expect("Expected result to be Ok");
            assert_eq!(result.shard_id, "shard-1");
            assert_eq!(result.last_checkpoint.sequence_number, "101");

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(!state.active.contains_key("shard-1"));
        }

        #[test]
        fn test_integration_parent_child_promotion() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Discover parent and child
            state
                .add_discovered(vec![
                    create_api_shard("parent", None, None),
                    create_api_shard("child", Some("parent"), None),
                ])
                .unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("parent"));
            assert!(state.blocked.contains_key("child"));

            // Activate parent
            state.mark_active("parent".to_string(), "parent-iter".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("parent"));
            assert!(state.blocked.contains_key("child"));

            let active_shard = state
                .active
                .get("parent")
                .expect("parent should be in active");
            assert_eq!(active_shard.shard_id, "parent");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "parent-iter");

            // Exhaust parent
            let batch = state.handle_poll_result("parent", None, vec![create_record("100")]);
            batch.unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(!state.active.contains_key("parent"));
            assert!(!state.blocked.contains_key("child"));
            assert!(state.initializing.contains_key("child"));

            // Activate child
            state.mark_active("child".to_string(), "child-iter".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("child"));

            let active_shard = state
                .active
                .get("child")
                .expect("child should be in active");
            assert_eq!(active_shard.shard_id, "child");
            assert_eq!(active_shard.parent_shard_id, Some("parent".to_string()));
            assert_eq!(active_shard.iterator, "child-iter");
        }

        #[test]
        fn test_integration_multiple_generations() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add three generations
            state
                .add_discovered(vec![
                    create_api_shard("gen1", None, None),
                    create_api_shard("gen2", Some("gen1"), None),
                    create_api_shard("gen3", Some("gen2"), None),
                ])
                .unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("gen1"));
            assert!(state.blocked.contains_key("gen2"));
            assert!(state.blocked.contains_key("gen3"));

            // Activate gen1
            state.mark_active("gen1".to_string(), "iter1".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            // Exhaust gen1
            state
                .handle_poll_result("gen1", None, vec![create_record("100")])
                .unwrap();
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("gen2"));
            assert!(state.blocked.contains_key("gen3"));

            // Activate gen2
            state.mark_active("gen2".to_string(), "iter2".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 0);

            // Exhaust gen2
            state
                .handle_poll_result("gen2", None, vec![create_record("200")])
                .unwrap();
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("gen3"));

            // Activate gen3
            state.mark_active("gen3".to_string(), "iter3".to_string());
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("gen3"));

            let active_shard = state.active.get("gen3").expect("gen3 should be in active");
            assert_eq!(active_shard.shard_id, "gen3");
            assert_eq!(active_shard.parent_shard_id, Some("gen2".to_string()));
            assert_eq!(active_shard.iterator, "iter3");
        }

        #[test]
        fn test_integration_shard_split_two_children() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Parent splits into two children
            state
                .add_discovered(vec![
                    create_api_shard("parent", None, None),
                    create_api_shard("child-a", Some("parent"), None),
                    create_api_shard("child-b", Some("parent"), None),
                ])
                .unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 1);

            state.mark_active("parent".to_string(), "parent-iter".to_string());

            // Both children should be pending
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.blocked.contains_key("child-a"));
            assert!(state.blocked.contains_key("child-b"));

            // Exhaust parent
            state
                .handle_poll_result("parent", None, vec![create_record("100")])
                .unwrap();

            // Both children should now be initializing
            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 2);
            assert!(state.initializing.contains_key("child-a"));
            assert!(state.initializing.contains_key("child-b"));

            // Activate both
            state.mark_active("child-a".to_string(), "iter-a".to_string());
            state.mark_active("child-b".to_string(), "iter-b".to_string());

            assert_eq!(state.active.len(), 2);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
            assert!(state.active.contains_key("child-a"));
            assert!(state.active.contains_key("child-b"));

            let child_a = state
                .active
                .get("child-a")
                .expect("child-a should be in active");
            assert_eq!(child_a.shard_id, "child-a");
            assert_eq!(child_a.parent_shard_id, Some("parent".to_string()));
            assert_eq!(child_a.iterator, "iter-a");

            let child_b = state
                .active
                .get("child-b")
                .expect("child-b should be in active");
            assert_eq!(child_b.shard_id, "child-b");
            assert_eq!(child_b.parent_shard_id, Some("parent".to_string()));
            assert_eq!(child_b.iterator, "iter-b");
        }

        #[test]
        fn test_integration_rediscovery_of_existing_shards() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Add initial shard
            state
                .add_discovered(vec![create_api_shard("shard-1", None, None)])
                .unwrap();
            state.mark_active("shard-1".to_string(), "iter-1".to_string());

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            // Rediscover same shard - should be ignored
            state
                .add_discovered(vec![create_api_shard("shard-1", None, None)])
                .unwrap();

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, None);
            assert_eq!(active_shard.iterator, "iter-1");
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: Some("parent".to_string()),
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            let records = vec![
                create_record("100"),
                create_record("101"),
                create_record("102"),
                create_record("103"),
                create_record("104"),
            ];

            let result = state.handle_poll_result("shard-1", Some("iter-2".to_string()), records);

            assert!(result.is_ok());
            let result = result.expect("Expected result to be Ok");
            assert_eq!(result.shard_id, "shard-1");
            if let PollOutcome::Records { records } = result.outcome {
                assert_eq!(records.len(), 5);
            } else {
                panic!("Expected PollOutcome::Records");
            }
            assert_eq!(result.last_checkpoint.sequence_number, "104");
            assert_eq!(result.last_checkpoint.parent_id, Some("parent".to_string()));
            assert_eq!(result.last_checkpoint.position, CheckpointPosition::After);

            // Verify state
            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);

            let active_shard = state
                .active
                .get("shard-1")
                .expect("shard-1 should be in active");
            assert_eq!(active_shard.shard_id, "shard-1");
            assert_eq!(active_shard.parent_shard_id, Some("parent".to_string()));
            assert_eq!(active_shard.iterator, "iter-2");
        }

        #[test]
        fn test_edge_case_empty_parent_id() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            let shard = BlockedShard {
                last_checkpoint: ShardCheckpoint {
                    sequence_number: String::new(),
                    parent_id: None,
                    updated_at: SystemTime::now(),
                    position: CheckpointPosition::After,
                },
                shard_id: "shard-1".to_string(),
                parent_shard_id: None,
            };

            state.try_move_to_initializing("shard-1", shard);

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 1);
            assert!(state.initializing.contains_key("shard-1"));

            let shard = state
                .initializing
                .get("shard-1")
                .expect("shard-1 should be in initializing");
            assert_eq!(shard.shard_id, "shard-1");
            assert_eq!(shard.parent_shard_id, None);
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
                    last_checkpoint: ShardCheckpoint {
                        sequence_number: String::new(),
                        parent_id: None,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::After,
                    },
                    last_produced_at: None,
                    current_watermark: None,
                },
            );

            // Should not panic
            state.promote_children("parent");

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 0);
            assert_eq!(state.initializing.len(), 0);
        }

        #[test]
        fn test_concurrent_children_from_different_parents() {
            let mut state = StreamState::new("arn:aws:stream:test".to_string());

            // Two independent parent-child chains
            state
                .add_discovered(vec![
                    create_api_shard("parent-1", None, None),
                    create_api_shard("parent-2", None, None),
                    create_api_shard("child-1", Some("parent-1"), None),
                    create_api_shard("child-2", Some("parent-2"), None),
                ])
                .unwrap();

            assert_eq!(state.active.len(), 0);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 2);

            state.mark_active("parent-1".to_string(), "iter1".to_string());
            state.mark_active("parent-2".to_string(), "iter2".to_string());

            assert_eq!(state.active.len(), 2);
            assert_eq!(state.blocked.len(), 2);
            assert_eq!(state.initializing.len(), 0);

            // Exhaust parent-1
            state
                .handle_poll_result("parent-1", None, vec![create_record("100")])
                .unwrap();

            assert_eq!(state.active.len(), 1);
            assert_eq!(state.blocked.len(), 1);
            assert_eq!(state.initializing.len(), 1);

            assert!(state.initializing.contains_key("child-1"));
            assert!(state.blocked.contains_key("child-2"));
            assert!(state.active.contains_key("parent-2"));
            assert!(!state.active.contains_key("parent-1"));

            let child1 = state
                .initializing
                .get("child-1")
                .expect("child-1 should be in initializing");
            assert_eq!(child1.shard_id, "child-1");
            assert_eq!(child1.parent_shard_id, Some("parent-1".to_string()));

            let child2 = state
                .blocked
                .get("child-2")
                .expect("child-2 should be in pending");
            assert_eq!(child2.shard_id, "child-2");
            assert_eq!(child2.parent_shard_id, Some("parent-2".to_string()));

            let parent2 = state
                .active
                .get("parent-2")
                .expect("parent-2 should be in active");
            assert_eq!(parent2.shard_id, "parent-2");
            assert_eq!(parent2.parent_shard_id, None);
            assert_eq!(parent2.iterator, "iter2");
        }
    }
}
