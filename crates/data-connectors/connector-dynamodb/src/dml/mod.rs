/*
Copyright 2026 The Spice.ai OSS Authors

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

pub mod delete;
pub mod insert;
pub(crate) mod keys;
pub mod update;

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodb::{Client as DbClient, types::WriteRequest};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Maximum number of items per `BatchWriteItem` request (`DynamoDB` limit).
pub(crate) const DYNAMODB_BATCH_WRITE_MAX: usize = 25;

/// Default number of concurrent `BatchWriteItem` requests.
pub const DEFAULT_WRITE_PARALLELISM: usize = 5;

/// Maximum number of retries for unprocessed items.
const MAX_UNPROCESSED_RETRIES: usize = 8;

/// Base delay for exponential backoff on unprocessed items.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(50);

/// Send a single `BatchWriteItem` request with retry logic for unprocessed items.
///
/// `DynamoDB` may return `UnprocessedItems` when throughput is exceeded.
/// We retry with exponential backoff until all items are processed or max retries reached.
async fn batch_write_with_retry(
    client: &DbClient,
    table_name: &str,
    items: Vec<WriteRequest>,
) -> DataFusionResult<()> {
    let mut current_items = items;

    for attempt in 0..=MAX_UNPROCESSED_RETRIES {
        let response = client
            .batch_write_item()
            .request_items(table_name, current_items)
            .send()
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("DynamoDB BatchWriteItem failed: {e}"))
            })?;

        let unprocessed = response
            .unprocessed_items
            .as_ref()
            .and_then(|m| m.get(table_name))
            .cloned()
            .unwrap_or_default();

        if unprocessed.is_empty() {
            return Ok(());
        }

        if attempt == MAX_UNPROCESSED_RETRIES {
            return Err(DataFusionError::Execution(format!(
                "DynamoDB BatchWriteItem: {} unprocessed items after {MAX_UNPROCESSED_RETRIES} retries",
                unprocessed.len()
            )));
        }

        let delay =
            RETRY_BASE_DELAY * 2u32.saturating_pow(u32::try_from(attempt).unwrap_or(u32::MAX));
        tokio::time::sleep(delay).await;
        current_items = unprocessed;
    }

    Ok(())
}

/// Drain all remaining tasks from a `JoinSet`, returning the first error encountered.
async fn drain_join_set(join_set: &mut JoinSet<DataFusionResult<()>>) -> DataFusionResult<()> {
    let mut first_error: Option<DataFusionError> = None;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Err(e)) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(e) if !e.is_cancelled() => {
                if first_error.is_none() {
                    first_error = Some(DataFusionError::Execution(format!("Task join error: {e}")));
                }
            }
            Ok(Ok(())) | Err(_) => {} // success or cancelled tasks
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Consume a stream of `WriteRequest` items, batch them into groups of 25, and write
/// to `DynamoDB` using a fixed pool of worker tasks. Cancels all workers on first error.
///
/// Returns the total number of items written.
pub(crate) async fn streaming_batch_write(
    client: &Arc<DbClient>,
    table_name: &str,
    mut requests: impl Stream<Item = DataFusionResult<WriteRequest>> + Send + Unpin,
    parallelism: usize,
) -> DataFusionResult<u64> {
    let (tx, rx) = mpsc::channel::<Vec<WriteRequest>>(parallelism * 2);

    // Spawn worker pool — each worker shares the receiver via a wrapper
    // Since mpsc::Receiver is not Clone, we use a single receiver with a Mutex
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let mut join_set = JoinSet::new();
    for _ in 0..parallelism {
        let client = Arc::clone(client);
        let table_name = table_name.to_string();
        let rx = Arc::clone(&rx);

        join_set.spawn(async move {
            loop {
                let chunk = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                match chunk {
                    Some(items) => {
                        batch_write_with_retry(&client, &table_name, items).await?;
                    }
                    None => return Ok(()), // channel closed
                }
            }
        });
    }

    let mut pending: Vec<WriteRequest> = Vec::with_capacity(DYNAMODB_BATCH_WRITE_MAX);
    let mut count: u64 = 0;

    // Producer loop: read from stream, buffer into chunks, send to workers
    let producer_result: DataFusionResult<()> = async {
        while let Some(item) = requests.next().await {
            let item = item?;
            pending.push(item);
            count += 1;

            if pending.len() >= DYNAMODB_BATCH_WRITE_MAX {
                // Check for worker errors before sending more work
                if let Some(result) = join_set.try_join_next() {
                    match result {
                        Ok(Err(e)) => return Err(e),
                        Err(e) if !e.is_cancelled() => {
                            return Err(DataFusionError::Execution(format!(
                                "Worker task error: {e}"
                            )));
                        }
                        Ok(Ok(())) | Err(_) => {}
                    }
                }

                let chunk =
                    std::mem::replace(&mut pending, Vec::with_capacity(DYNAMODB_BATCH_WRITE_MAX));
                tx.send(chunk).await.map_err(|_| {
                    DataFusionError::Execution(
                        "Failed to send chunk to worker: channel closed".to_string(),
                    )
                })?;
            }
        }

        // Flush remaining items
        if !pending.is_empty() {
            tx.send(pending).await.map_err(|_| {
                DataFusionError::Execution(
                    "Failed to send final chunk to worker: channel closed".to_string(),
                )
            })?;
        }

        Ok(())
    }
    .await;

    // Drop the sender so workers see channel closed and exit
    drop(tx);

    // If producer had an error, abort workers and drain
    if let Err(e) = producer_result {
        join_set.abort_all();
        // Drain to ensure all tasks are stopped before returning
        let _ = drain_join_set(&mut join_set).await;
        return Err(e);
    }

    // Wait for all workers to finish
    drain_join_set(&mut join_set).await?;

    Ok(count)
}
