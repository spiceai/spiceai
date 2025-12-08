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
use crate::checkpoint::{Checkpoint, CheckpointPosition};
use crate::client_sdk::SDKClient;
use crate::metrics::MetricsCollector;
use crate::stream_state::{InitializingShard, PollOutcome, ShardPollResult, StreamState};
use crate::{Result, StreamResult};
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use futures::{Stream, future::join_all};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::SystemTime;
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
    pub metrics_collector: Arc<MetricsCollector>,
}

pub struct DynamoDBStreamBatch {
    pub records: Vec<Record>,
    pub checkpoint: Checkpoint,
    pub watermark: Option<SystemTime>,
}

impl DynamodbStreamProducer {
    async fn collect(&mut self) -> Result<(DynamoDBStreamBatch, bool)> {
        let mut poll_results = Vec::new();
        let mut had_transient_error = false;

        // 1. Initialize shards that require iterators
        // If permanent error is encountered, it is surfaced to the client.
        self.initialize_shards_iterators().await?;

        // 2. Poll active shards
        let futures = self.state.get_active_shards().map(|shard| {
            let client = Arc::clone(&self.client);
            tracing::trace!(
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
            let poll_result = match result {
                Ok((next_iter, records)) => self
                    .state
                    .handle_poll_result(&shard_id, next_iter, records)?,
                Err(e) => {
                    had_transient_error = true;
                    self.state.handle_poll_error(&shard_id, e)?
                }
            };
            poll_results.push(poll_result);
        }

        // 4. Discover new shards
        // If permanent error is encountered, it is surfaced to the client.
        match self.client.get_all_shards(&self.stream_arn).await {
            Ok(shards) => self.state.add_discovered(&shards)?,
            Err(e) => {
                if !e.is_retriable() {
                    return Err(e);
                }
                had_transient_error = true;
                tracing::warn!("Failed to discover new shards. Will retry on next iteration: {e}");
            }
        }

        Ok((combine_shard_batches(&poll_results), had_transient_error))
    }

    async fn initialize_shards_iterators(&mut self) -> Result<()> {
        let shards: Vec<InitializingShard> =
            self.state.get_initializing_shards().cloned().collect();

        for shard in shards {
            // Shards that were already polled use `After`.
            // Newly discovered shards use `At`.
            let iterator_type = match shard.last_checkpoint.position {
                CheckpointPosition::At => ShardIteratorType::AtSequenceNumber,
                CheckpointPosition::After => ShardIteratorType::AfterSequenceNumber,
            };

            match self
                .client
                .get_shard_iterator(
                    &self.stream_arn,
                    &shard.shard_id,
                    &iterator_type,
                    Some(shard.last_checkpoint.sequence_number.clone()),
                )
                .await
            {
                Ok(iterator) => {
                    self.state.mark_active(shard.shard_id, iterator);
                }
                Err(e) => {
                    if !e.is_retriable() {
                        return Err(e);
                    }
                    tracing::warn!(
                        "Failed to initialize shard. Will retry on next iteration : {}",
                        e
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn streaming(mut self) {
        let mut backoff = self.retry_strategy.clone();

        loop {
            if let Ok(mut guard) = self.metrics_collector.active_shards_number.write() {
                *guard = self.state.get_active_shards().count();
            }

            match self.collect().await {
                Ok((batch, had_transient_error)) => {
                    if !batch.records.is_empty() {
                        self.metrics_collector
                            .records
                            .fetch_add(batch.records.len(), Ordering::Relaxed);
                    }

                    if let Some(watermark) = batch.watermark
                        && let Ok(mut wm) = self.metrics_collector.watermark.write()
                    {
                        *wm = Some(watermark);
                    }

                    if had_transient_error {
                        self.metrics_collector
                            .transient_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    // Send batch if it has records
                    if !batch.records.is_empty() && self.sender.send(Ok(batch)).await.is_err() {
                        return;
                    }

                    if had_transient_error {
                        // Transient error occurred during collection - apply backoff
                        if let Some(mut duration) = backoff.next_backoff() {
                            // Avoid sleeping for more than 60 seconds
                            if duration > Duration::from_secs(60) {
                                duration = Duration::from_secs(1);
                                backoff.reset();
                            }
                            tokio::time::sleep(duration).await;
                        } else {
                            // Backoff exhausted - transient errors persisted too long
                            // Shouldn't happen as we should have infinite retries.
                            return;
                        }
                    } else {
                        // Clean success - reset backoff and use normal interval
                        backoff = self.retry_strategy.clone();

                        if let Some(duration) = self.interval {
                            sleep(duration).await;
                        }
                    }
                }
                Err(e) => {
                    // Permanent error - return immediately without retry
                    let _ = self.sender.send(Err(e)).await;
                    return;
                }
            }
        }
    }
}

fn combine_shard_batches(poll_results: &[ShardPollResult]) -> DynamoDBStreamBatch {
    // Collect records, checkpoints and watermarks
    let mut records = Vec::new();
    let mut shard_watermarks = Vec::new();
    let mut shards_checkpoints = HashMap::new();

    for shard_result in poll_results {
        // Collect records
        if let PollOutcome::Records {
            records: shard_records,
        } = &shard_result.outcome
        {
            records.extend(shard_records.clone());
        }

        // Collect checkpoints
        shards_checkpoints.insert(
            shard_result.shard_id.clone(),
            shard_result.last_checkpoint.clone(),
        );

        // Check eligibility for watermark
        let is_eligible = match shard_result.outcome {
            // Shards that produced records and those that failed are always eligible
            PollOutcome::Records { .. } | PollOutcome::Failed => true,

            // Shards that produced no records are NOT eligible as there's no lag
            PollOutcome::Empty => false,
        };

        // If eligible, include its current_watermark
        if is_eligible && let Some(watermark) = shard_result.current_watermark {
            tracing::trace!(
                "Shard {} included in watermark: {}",
                shard_result.shard_id,
                humantime::format_rfc3339(watermark),
            );
            shard_watermarks.push(watermark);
        }
    }

    let watermark = if shard_watermarks.is_empty() {
        tracing::trace!("No eligible shards with watermarks, watermark is None");
        None
    } else {
        let min_watermark = shard_watermarks.into_iter().min();
        tracing::trace!("Calculated watermark: {:?}", min_watermark);
        min_watermark
    };

    DynamoDBStreamBatch {
        records,
        checkpoint: Checkpoint {
            shards: shards_checkpoints,
        },
        watermark,
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
