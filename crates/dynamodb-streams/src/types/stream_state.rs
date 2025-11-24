use crate::client_sdk::SDKClient;
use crate::types::checkpoint::{CheckpointPosition, GlobalCheckpoint, ShardCheckpoint};
use crate::types::shard::ApiShard;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug)]
pub struct RecordBatch {
    pub shard_id: String,
    pub records: Vec<Record>,
    pub checkpoint: ShardCheckpoint,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct PendingShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
}

#[derive(Debug)]
pub struct StreamState {
    stream_arn: String,
    active: HashMap<String, ActiveShard>,
    pending: HashMap<String, PendingShard>,
    pub initializing: HashMap<String, PendingShard>, // TODO: remove pub
}

impl StreamState {
    pub fn new(stream_arn: String) -> Self {
        Self {
            stream_arn,
            active: HashMap::new(),
            pending: HashMap::new(),
            initializing: HashMap::new(),
        }
    }

    pub fn active_shards(&self) -> impl Iterator<Item = &ActiveShard> {
        self.active.values()
    }

    pub fn handle_poll_result(
        &mut self,
        shard_id: &str,
        new_iterator: Option<String>,
        records: Vec<Record>,
    ) -> Option<RecordBatch> {
        tracing::debug!(
            "Processing shard poll: shard_id={:?}, new_iterator={:?}, records_num={:?}",
            shard_id,
            new_iterator,
            records.len()
        );

        let parent_id = self.active.get(shard_id)?.parent_shard_id.clone();

        match new_iterator {
            Some(iter) => {
                self.active.get_mut(shard_id)?.set_iterator(iter);
            }
            None => {
                self.active.remove(shard_id);
                self.promote_children(shard_id)
            }
        };

        if records.is_empty() {
            return None;
        }

        let last_seq_opt = records.last()?.clone().dynamodb?.sequence_number.clone();
        tracing::debug!(
            "Shard latest sequence number: shard_id={:?}, seq_number={:?}",
            shard_id,
            last_seq_opt
        );
        if last_seq_opt.is_none() {
            tracing::error!("Missing sequence number: shard_id={}", shard_id);
        }
        let last_seq = last_seq_opt?;

        Some(RecordBatch {
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

    /// Add discovered shards, returns shard IDs that need initialization
    pub fn add_discovered(&mut self, shards: Vec<ApiShard>) {
        for shard in shards {
            let shard_id = shard.id().to_string();

            if self.active.contains_key(&shard_id)
                || self.pending.contains_key(&shard_id)
                || self.initializing.contains_key(&shard_id)
            {
                continue;
            }

            let blocked = shard
                .parent_id()
                .map(|p| {
                    self.active.contains_key(p)
                        || self.pending.contains_key(p)
                        || self.initializing.contains_key(p)
                })
                .unwrap_or(false);

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
                self.try_move_to_initializing(child_id, child);
            }
        }
    }

    fn try_move_to_initializing(
        &mut self,
        shard_id: String,
        shard: PendingShard,
    ) {
        let is_blocked = shard
            .parent_shard_id
            .as_ref()
            .map(|p| {
                self.active.contains_key(p)
                    || self.pending.contains_key(p)
                    || self.initializing.contains_key(p)
            })
            .unwrap_or(false);

        if is_blocked {
            self.pending.insert(shard_id.clone(), shard);
        } else {
            self.initializing.insert(shard_id.clone(), shard);
        }
    }
}

pub async fn initialize_state_from_checkpoint(
    checkpoint: &GlobalCheckpoint,
    sdk_client: Arc<SDKClient>,
) -> crate::Result<StreamState> {
    let mut state = StreamState {
        stream_arn: checkpoint.stream_arn.clone(),
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
                &checkpoint.stream_arn,
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
        if child.parent_id() == Some(parent_id) {
            if let Ok(Some(iterator)) = sdk_client
                .get_shard_iterator(
                    &state.stream_arn,
                    child.id(),
                    &ShardIteratorType::TrimHorizon,
                    None,
                )
                .await
            {
                let shard = ActiveShard {
                    shard_id: child.id().to_string(),
                    parent_shard_id: Some(parent_id.to_string()),
                    iterator,
                };
                state.active.insert(child.id().to_string(), shard);
            } else {
                //TODO: Else what?
            }
        }
    }

    Ok(())
}
