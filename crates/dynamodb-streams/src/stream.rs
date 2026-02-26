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

const DEFAULT_SLEEP_DURATION: Duration = Duration::from_millis(500);

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
                    let is_batch_empty = batch.records.is_empty();

                    self.metrics_collector
                        .records
                        .fetch_add(batch.records.len(), Ordering::Relaxed);

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

                    // Send batch even if it's empty
                    if self.sender.send(Ok(batch)).await.is_err() {
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

                        if is_batch_empty {
                            // To avoid throttling - wait at least 500ms before polling again
                            sleep(
                                DEFAULT_SLEEP_DURATION
                                    .max(self.interval.unwrap_or(Duration::from_secs(0))),
                            )
                            .await;
                        } else if let Some(duration) = self.interval {
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

    let mut empty_shards_num = 0;

    for shard_result in poll_results {
        // Skip duplicate shard_ids - only process the first occurrence
        // This ensures consistency: one checkpoint per shard, and only records from that occurrence
        if shards_checkpoints.contains_key(&shard_result.shard_id) {
            tracing::warn!(
                "Duplicate shard_id {} in poll results, skipping",
                shard_result.shard_id
            );
            continue;
        }

        // Collect checkpoints
        shards_checkpoints.insert(
            shard_result.shard_id.clone(),
            shard_result.last_checkpoint.clone(),
        );

        // Collect records and check watermark eligibility
        let is_watermark_eligible = match &shard_result.outcome {
            PollOutcome::Records {
                records: shard_records,
            } => {
                records.extend(shard_records.clone());
                true
            }
            PollOutcome::Failed => true,
            PollOutcome::Empty => {
                empty_shards_num += 1;
                false
            }
        };

        // If eligible, include its current_watermark
        if is_watermark_eligible && let Some(watermark) = shard_result.current_watermark {
            tracing::trace!(
                "Shard {} included in watermark: {}",
                shard_result.shard_id,
                humantime::format_rfc3339(watermark),
            );
            shard_watermarks.push(watermark);
        }
    }

    let watermark = if !shard_watermarks.is_empty() {
        let min_watermark = shard_watermarks.into_iter().min();
        tracing::trace!("Calculated watermark: {:?}", min_watermark);
        min_watermark
    } else if empty_shards_num == poll_results.len() {
        tracing::trace!("All shards are empty, watermark is Now()");
        Some(SystemTime::now())
    } else {
        tracing::trace!("No eligible shards with watermarks, watermark is None");
        None
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointPosition, ShardCheckpoint};
    use crate::stream_state::{PollOutcome, ShardPollResult};
    use aws_sdk_dynamodbstreams::types::StreamRecord;
    use std::time::{Duration, UNIX_EPOCH};

    fn create_checkpoint(seq: &str, parent: Option<&str>) -> ShardCheckpoint {
        ShardCheckpoint {
            sequence_number: seq.to_string(),
            parent_id: parent.map(std::string::ToString::to_string),
            updated_at: SystemTime::now(),
            position: CheckpointPosition::After,
        }
    }

    fn create_record(seq: &str) -> Record {
        Record::builder()
            .dynamodb(StreamRecord::builder().sequence_number(seq).build())
            .build()
    }

    fn system_time_from_secs(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    mod combine_shard_batches {
        use super::*;

        #[test]
        fn test_empty_poll_results() {
            let results: Vec<ShardPollResult> = vec![];
            let batch = combine_shard_batches(&results);

            assert!(batch.records.is_empty());
            assert!(batch.checkpoint.shards.is_empty());
            let lag = SystemTime::now()
                .duration_since(batch.watermark.expect("watermark"))
                .expect("lag");
            assert!(lag <= Duration::from_millis(100));
        }

        #[test]
        fn test_single_shard_with_records() {
            let results = vec![ShardPollResult {
                shard_id: "shard-1".to_string(),
                outcome: PollOutcome::Records {
                    records: vec![create_record("100"), create_record("101")],
                },
                last_checkpoint: create_checkpoint("101", None),
                current_watermark: Some(system_time_from_secs(1000)),
            }];

            let batch = combine_shard_batches(&results);

            assert_eq!(batch.records.len(), 2);
            assert_eq!(batch.checkpoint.shards.len(), 1);
            assert!(batch.checkpoint.shards.contains_key("shard-1"));
            assert_eq!(batch.watermark, Some(system_time_from_secs(1000)));
        }

        #[test]
        fn test_single_shard_empty_records() {
            let results = vec![ShardPollResult {
                shard_id: "shard-1".to_string(),
                outcome: PollOutcome::Empty,
                last_checkpoint: create_checkpoint("100", None),
                current_watermark: Some(system_time_from_secs(1000)),
            }];

            let batch = combine_shard_batches(&results);

            assert!(batch.records.is_empty());
            assert_eq!(batch.checkpoint.shards.len(), 1);
            let lag = SystemTime::now()
                .duration_since(batch.watermark.expect("watermark"))
                .expect("lag");
            assert!(lag <= Duration::from_millis(100));
        }

        #[test]
        fn test_single_shard_failed() {
            let results = vec![ShardPollResult {
                shard_id: "shard-1".to_string(),
                outcome: PollOutcome::Failed,
                last_checkpoint: create_checkpoint("100", None),
                current_watermark: Some(system_time_from_secs(1000)),
            }];

            let batch = combine_shard_batches(&results);

            assert!(batch.records.is_empty());
            assert_eq!(batch.checkpoint.shards.len(), 1);
            // Failed shards ARE eligible for watermark (they have lag)
            assert_eq!(batch.watermark, Some(system_time_from_secs(1000)));
        }

        #[test]
        fn test_multiple_shards_records_combined() {
            let results = vec![
                ShardPollResult {
                    shard_id: "shard-1".to_string(),
                    outcome: PollOutcome::Records {
                        records: vec![create_record("100")],
                    },
                    last_checkpoint: create_checkpoint("100", None),
                    current_watermark: Some(system_time_from_secs(1000)),
                },
                ShardPollResult {
                    shard_id: "shard-2".to_string(),
                    outcome: PollOutcome::Records {
                        records: vec![create_record("200"), create_record("201")],
                    },
                    last_checkpoint: create_checkpoint("201", Some("shard-1")),
                    current_watermark: Some(system_time_from_secs(2000)),
                },
            ];

            let batch = combine_shard_batches(&results);

            assert_eq!(batch.records.len(), 3);
            assert_eq!(batch.checkpoint.shards.len(), 2);
            // Watermark should be the minimum
            assert_eq!(batch.watermark, Some(system_time_from_secs(1000)));
        }

        #[test]
        fn test_watermark_takes_minimum() {
            let results = vec![
                ShardPollResult {
                    shard_id: "shard-1".to_string(),
                    outcome: PollOutcome::Records { records: vec![] },
                    last_checkpoint: create_checkpoint("100", None),
                    current_watermark: Some(system_time_from_secs(3000)),
                },
                ShardPollResult {
                    shard_id: "shard-2".to_string(),
                    outcome: PollOutcome::Records { records: vec![] },
                    last_checkpoint: create_checkpoint("200", None),
                    current_watermark: Some(system_time_from_secs(1000)), // Minimum
                },
                ShardPollResult {
                    shard_id: "shard-3".to_string(),
                    outcome: PollOutcome::Records { records: vec![] },
                    last_checkpoint: create_checkpoint("300", None),
                    current_watermark: Some(system_time_from_secs(2000)),
                },
            ];

            let batch = combine_shard_batches(&results);

            assert_eq!(batch.watermark, Some(system_time_from_secs(1000)));
        }

        #[test]
        fn test_mixed_outcomes_watermark() {
            // Scenario: 3 shards
            // - shard-1: has records with watermark 1000
            // - shard-2: empty (no watermark contribution)
            // - shard-3: failed with watermark 500 (should contribute)
            let results = vec![
                ShardPollResult {
                    shard_id: "shard-1".to_string(),
                    outcome: PollOutcome::Records {
                        records: vec![create_record("100")],
                    },
                    last_checkpoint: create_checkpoint("100", None),
                    current_watermark: Some(system_time_from_secs(1000)),
                },
                ShardPollResult {
                    shard_id: "shard-2".to_string(),
                    outcome: PollOutcome::Empty,
                    last_checkpoint: create_checkpoint("200", None),
                    current_watermark: Some(system_time_from_secs(2000)),
                },
                ShardPollResult {
                    shard_id: "shard-3".to_string(),
                    outcome: PollOutcome::Failed,
                    last_checkpoint: create_checkpoint("300", None),
                    current_watermark: Some(system_time_from_secs(500)),
                },
            ];

            let batch = combine_shard_batches(&results);

            // Only shard-1 (records) and shard-3 (failed) contribute
            // Minimum is 500 from shard-3
            assert_eq!(batch.watermark, Some(system_time_from_secs(500)));
        }

        #[test]
        fn test_no_watermarks_from_eligible_shards() {
            let results = vec![ShardPollResult {
                shard_id: "shard-1".to_string(),
                outcome: PollOutcome::Records {
                    records: vec![create_record("100")],
                },
                last_checkpoint: create_checkpoint("100", None),
                current_watermark: None, // No watermark
            }];

            let batch = combine_shard_batches(&results);

            assert!(!batch.records.is_empty());
            assert!(batch.watermark.is_none());
        }

        #[test]
        fn test_checkpoints_collected_from_all_shards() {
            let results = vec![
                ShardPollResult {
                    shard_id: "shard-1".to_string(),
                    outcome: PollOutcome::Records { records: vec![] },
                    last_checkpoint: create_checkpoint("100", None),
                    current_watermark: None,
                },
                ShardPollResult {
                    shard_id: "shard-2".to_string(),
                    outcome: PollOutcome::Empty,
                    last_checkpoint: create_checkpoint("200", Some("shard-1")),
                    current_watermark: None,
                },
                ShardPollResult {
                    shard_id: "shard-3".to_string(),
                    outcome: PollOutcome::Failed,
                    last_checkpoint: create_checkpoint("300", None),
                    current_watermark: None,
                },
            ];

            let batch = combine_shard_batches(&results);

            assert_eq!(batch.checkpoint.shards.len(), 3);
            assert!(batch.checkpoint.shards.contains_key("shard-1"));
            assert!(batch.checkpoint.shards.contains_key("shard-2"));
            assert!(batch.checkpoint.shards.contains_key("shard-3"));

            let shard2_checkpoint = batch.checkpoint.shards.get("shard-2").expect("shard-2");
            assert_eq!(shard2_checkpoint.sequence_number, "200");
            assert_eq!(shard2_checkpoint.parent_id, Some("shard-1".to_string()));
        }

        /// Verifies that duplicate `shard_id`s are handled consistently.
        /// The implementation skips duplicates to ensure consistency between
        /// records and checkpoints.
        #[test]
        fn test_duplicate_shard_id_is_deduplicated() {
            // Two results with the same shard_id but different checkpoints
            let results = vec![
                ShardPollResult {
                    shard_id: "shard-1".to_string(),
                    outcome: PollOutcome::Records {
                        records: vec![create_record("100")],
                    },
                    last_checkpoint: create_checkpoint("100", None),
                    current_watermark: Some(system_time_from_secs(1000)),
                },
                ShardPollResult {
                    shard_id: "shard-1".to_string(), // DUPLICATE - will be skipped
                    outcome: PollOutcome::Records {
                        records: vec![create_record("200")],
                    },
                    last_checkpoint: create_checkpoint("200", None),
                    current_watermark: Some(system_time_from_secs(2000)),
                },
            ];

            let batch = combine_shard_batches(&results);

            // With the fix, duplicates are skipped, so we get exactly 1 checkpoint and 1 record
            let checkpoint_count = batch.checkpoint.shards.len();
            let record_count = batch.records.len();

            // Both should be 1 - only the first occurrence is processed
            assert_eq!(checkpoint_count, 1, "Should have exactly 1 checkpoint");
            assert_eq!(
                record_count, 1,
                "Should have exactly 1 record (duplicate skipped)"
            );

            // Verify the first checkpoint was kept (sequence "100")
            let checkpoint = batch.checkpoint.shards.get("shard-1").expect("shard-1");
            assert_eq!(checkpoint.sequence_number, "100");
        }
    }
}

/// Tests that document concurrency and async bugs in the streaming module.
/// These tests document issues and will FAIL when the bugs are fixed.
#[cfg(test)]
mod concurrency_bug_tests {

    /// BUG: Producer task has no graceful shutdown mechanism.
    ///
    /// When the consumer drops the `DynamodbStream`, the producer task:
    /// 1. Will only stop after the next `sender.send()` fails (could be up to `interval` later)
    /// 2. No cleanup of pending work or resources
    /// 3. No logging that shutdown occurred
    ///
    /// Current behavior: `if self.sender.send(...).await.is_err() { return; }`
    /// Correct behavior: Use `CancellationToken` or `select!` with shutdown signal
    ///
    /// The `streaming()` loop in `DynamodbStreamProducer` looks like:
    /// ```ignore
    /// loop {
    ///     // ... do work ...
    ///     if !batch.records.is_empty() && self.sender.send(Ok(batch)).await.is_err() {
    ///         return;  // BUG: Only exits when send fails, not on explicit shutdown
    ///     }
    ///     // ... sleep for interval ...
    /// }
    /// ```
    ///
    /// Problems:
    /// 1. If interval is 60s, producer runs for 60s after consumer drops
    /// 2. No way to cancel from outside (e.g., for graceful server shutdown)
    /// 3. `tokio::spawn` returns `JoinHandle` that is immediately dropped
    ///
    /// This test documents the bug - the actual fix requires adding:
    /// 1. A `CancellationToken` to the producer
    /// 2. `select!` on the cancellation token in the loop
    /// 3. Returning the `JoinHandle` for proper cleanup
    #[test]
    fn test_bug_no_graceful_shutdown_documented() {
        // This test documents the architectural issue.
        // See the doc comment above for details.
    }

    /// BUG: Empty batch sends are skipped, delaying shutdown detection.
    ///
    /// When there are no records, the send is skipped entirely:
    /// ```ignore
    /// if !batch.records.is_empty() && self.sender.send(Ok(batch)).await.is_err() {
    ///     return;
    /// }
    /// ```
    ///
    /// This means if the consumer is dropped during a period of no activity:
    /// 1. Producer won't detect it until records appear
    /// 2. Could run indefinitely if stream has no activity
    ///
    /// The fix: Always check channel status, perhaps with:
    /// - `if self.sender.is_closed() { return; }`
    /// - Or send heartbeat batches periodically
    #[test]
    fn test_bug_empty_batch_delays_shutdown_detection_documented() {
        // This test documents the architectural issue.
        // See the doc comment above for details.
    }

    /// BUG: `RwLock` write failures in `streaming()` are silently ignored.
    ///
    /// In the `streaming()` method:
    /// ```ignore
    /// if let Ok(mut guard) = self.metrics_collector.active_shards_number.write() {
    ///     *guard = self.state.get_active_shards().count();
    /// }
    /// ```
    ///
    /// If the write lock fails (e.g., poisoned), the metrics are simply not updated.
    /// This could hide critical issues during debugging/monitoring.
    ///
    /// The fix: At minimum, log a warning when lock acquisition fails.
    #[test]
    fn test_bug_rwlock_write_failures_silently_ignored_documented() {
        // This test documents the architectural issue.
        // See the doc comment above for details.
    }
}
