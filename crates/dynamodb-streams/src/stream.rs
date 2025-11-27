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
use crate::checkpoint::Checkpoint;
use crate::client_sdk::SDKClient;
use crate::stream_state::{DynamoDBStreamBatch, ShardPollResult, StreamState};
use crate::{Error, Result, StreamResult};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use futures::{Stream, future::join_all};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};
use util::retry_strategy::{Backoff, RetryBackoff};

#[derive(Debug)]
pub struct DynamodbStreamProducer {
    pub stream_arn: String,
    pub state: StreamState,
    pub interval: Option<Duration>,
    pub sender: mpsc::Sender<StreamResult>,
    pub client: Arc<SDKClient>,
    pub retry_strategy: RetryBackoff,
}

impl DynamodbStreamProducer {
    async fn collect(&mut self) -> Result<DynamoDBStreamBatch> {
        let mut batches = Vec::new();

        // 1. Initialize shards that require iterators
        self.initialize_shards_iterators().await;

        // 2. Poll active shards
        let futures = self.state.get_active_shards().map(|shard| {
            let client = Arc::clone(&self.client);
            tracing::debug!(
                "Polling shard with iterator: shard_id={}, iterator={}",
                shard.shard_id,
                shard.iterator
            );
            async move {
                (
                    shard.shard_id.clone(),
                    client.get_iterator_records(&shard.iterator).await,
                )
            }
        });

        let results = join_all(futures).await;

        // 3. Process poll results
        for (shard_id, result) in results {
            match result {
                Ok((next_iter, records)) => {
                    if let Some(batch) =
                        self.state.handle_poll_result(&shard_id, next_iter, records)
                    {
                        batches.push(batch);
                    }
                }
                Err(e) => {
                    tracing::error!("Shard poll failed: shard_id={}, {}", shard_id, e);
                    self.handle_failed_shard(&shard_id, &e);
                }
            }
        }

        // 4. Discover new shards
        if let Ok(shards) = self.client.get_all_shards(&self.stream_arn).await {
            self.state.add_discovered(shards);
        }

        Ok(combine_shard_batches(batches))
    }

    fn handle_failed_shard(&mut self, shard_id: &str, error: &Error) {
        let is_expired_iterator = error.to_string().contains("ExpiredIterator")
            || error.to_string().contains("TrimmedDataAccess");

        if is_expired_iterator {
            tracing::warn!("Iterator expired for shard {}, will reinitialize", shard_id);

            self.state.reinitialize_shard(shard_id);
        }
    }

    async fn initialize_shards_iterators(&mut self) {
        let shard_ids: Vec<String> = self.state.get_initializing_shards_ids().cloned().collect();

        for shard_id in shard_ids {
            match self
                .client
                .get_shard_iterator(
                    &self.stream_arn,
                    &shard_id,
                    &ShardIteratorType::TrimHorizon,
                    None,
                )
                .await
            {
                Ok(iterator) => {
                    if let Some(iterator) = iterator {
                        self.state.mark_active(shard_id, iterator);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to initialize shard {}: {}", shard_id, e);
                }
            }
        }
    }

    async fn perform_iterate_with_retry(&mut self) -> Result<DynamoDBStreamBatch> {
        let mut backoff = self.retry_strategy.clone();

        loop {
            match self.collect().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if let Some(duration) = backoff.next_backoff() {
                        tracing::debug!("Iteration failed, retrying after {:?}: {}", duration, e);
                        tokio::time::sleep(duration).await;
                    } else {
                        tracing::error!("Iteration failed after exhausting retries: {}", e);
                        return Err(e);
                    }
                }
            }
        }
    }

    pub async fn streaming(mut self) {
        loop {
            let Ok(batch) = self.perform_iterate_with_retry().await else {
                // Error is logged in `perform_iterate_with_retry`
                return;
            };

            if self.sender.send(Ok(batch)).await.is_err() {
                return;
            }

            if let Some(duration) = self.interval {
                sleep(duration).await;
            }
        }
    }
}

fn combine_shard_batches(batches: Vec<ShardPollResult>) -> DynamoDBStreamBatch {
    let mut records = Vec::new();
    let mut shards_checkpoints = HashMap::new();

    for batch in batches {
        records.extend(batch.records);
        shards_checkpoints.insert(batch.shard_id, batch.checkpoint);
    }

    DynamoDBStreamBatch {
        records,
        checkpoint: Checkpoint {
            shards: shards_checkpoints,
        },
    }
}

#[derive(Debug)]
pub struct DynamodbStream {
    pub receiver: mpsc::Receiver<StreamResult>,
}

impl Stream for DynamodbStream {
    type Item = StreamResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DynamodbStream {
    fn drop(&mut self) {
        self.receiver.close();
    }
}
