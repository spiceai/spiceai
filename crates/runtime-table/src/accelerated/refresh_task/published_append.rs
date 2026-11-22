/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Forwarding a committed append refresh to the dataset's `DoExchange`
//! subscribers.
//!
//! A `DoExchange` subscription (the `spice.ai` connector's append stream) is
//! fed by [`DataUpdatePublisher`]. Writes through `DoPut` publish to it, and so
//! must an accelerated `refresh_mode: append` refresh: otherwise a subscriber
//! receives the initial snapshot and then nothing, while the dataset it mirrors
//! keeps growing.
//!
//! The refresh streams its rows into the accelerator, so the rows are kept as
//! they pass, and sent only after the write returns: a subscriber never sees a
//! row a reader of the dataset cannot. Keeping them is bounded by
//! [`MAX_PUBLISHED_REFRESH_APPEND_BYTES`]; an append larger than that ends the
//! subscriptions instead, so a subscriber resubscribes from a fresh snapshot
//! rather than silently missing rows.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_tools::record_batch::slice_memory_size;
use datafusion::common::TableReference;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use runtime_acceleration::dataupdate::{
    CommitGuard, DataUpdatePublisher, MAX_PUBLISHED_REFRESH_APPEND_BYTES,
};

#[derive(Debug, Default)]
struct Kept {
    batches: Vec<RecordBatch>,
    bytes: usize,
    overflowed: bool,
}

/// The rows of one append write, kept for its subscribers while the write runs.
#[derive(Debug, Clone)]
pub(crate) struct PublishedAppend {
    kept: Arc<parking_lot::Mutex<Kept>>,
    max_bytes: usize,
}

/// What to send the subscribers once the write committed.
#[derive(Debug)]
pub(crate) enum KeptAppend {
    Rows(Vec<RecordBatch>),
    /// More than the bound was written; the subscribers cannot be sent it.
    Overflowed,
}

impl PublishedAppend {
    pub(crate) fn new() -> Self {
        Self::with_max_bytes(MAX_PUBLISHED_REFRESH_APPEND_BYTES)
    }

    pub(crate) fn with_max_bytes(max_bytes: usize) -> Self {
        Self {
            kept: Arc::new(parking_lot::Mutex::new(Kept::default())),
            max_bytes,
        }
    }

    /// `source`, keeping each batch it yields.
    pub(crate) fn keep(&self, source: SendableRecordBatchStream) -> SendableRecordBatchStream {
        let schema = source.schema();
        let kept = Arc::clone(&self.kept);
        let max_bytes = self.max_bytes;
        let kept_stream = source.inspect(move |batch| {
            let Ok(batch) = batch else {
                return;
            };
            if batch.num_rows() == 0 {
                return;
            }
            let mut kept = kept.lock();
            if kept.overflowed {
                return;
            }
            let bytes = kept.bytes.saturating_add(slice_memory_size(batch));
            if bytes > max_bytes {
                // Release what was kept now rather than holding it to the
                // end of the write.
                kept.batches = Vec::new();
                kept.bytes = 0;
                kept.overflowed = true;
                return;
            }
            kept.bytes = bytes;
            kept.batches.push(batch.clone());
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, kept_stream))
    }

    pub(crate) fn take(&self) -> KeptAppend {
        let mut kept = self.kept.lock();
        if kept.overflowed {
            return KeptAppend::Overflowed;
        }
        kept.bytes = 0;
        KeptAppend::Rows(std::mem::take(&mut kept.batches))
    }
}

/// Send a committed append to the subscribers of `publisher`'s dataset.
pub(crate) async fn publish_committed_append(
    publisher: &DataUpdatePublisher,
    commit_guard: CommitGuard,
    dataset_name: &TableReference,
    schema: SchemaRef,
    kept: KeptAppend,
) {
    match kept {
        KeptAppend::Rows(batches) => {
            publisher
                .publish_append(commit_guard, schema, batches)
                .await;
        }
        // The guard is held until the subscriptions are ended, so no one
        // subscribes in between and is ended for nothing.
        KeptAppend::Overflowed => {
            if publisher.close_subscribers().await {
                tracing::warn!(
                    "Dataset {dataset_name}: an append refresh wrote more than {} MiB, more than can be forwarded to its append-stream subscribers, so their subscriptions were ended. Each subscriber must resubscribe to read the dataset from a fresh snapshot.",
                    MAX_PUBLISHED_REFRESH_APPEND_BYTES / (1024 * 1024)
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryStream;
    use futures::TryStreamExt;
    use runtime_acceleration::dataupdate::{DataUpdateBroadcaster, UpdateType};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values))])
            .expect("valid batch")
    }

    fn stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        Box::pin(MemoryStream::try_new(batches, schema(), None).expect("memory stream"))
    }

    #[tokio::test]
    async fn keeps_every_batch_the_write_reads() {
        let published = PublishedAppend::new();
        let written: Vec<RecordBatch> = published
            .keep(stream(vec![
                batch(vec![1, 2]),
                batch(vec![]),
                batch(vec![3]),
            ]))
            .try_collect()
            .await
            .expect("stream");
        assert_eq!(written.len(), 3, "the write still reads every batch");

        let KeptAppend::Rows(kept) = published.take() else {
            panic!("a small append is kept");
        };
        let rows: usize = kept.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3);
        assert_eq!(kept.len(), 2, "empty batches are not kept");
    }

    #[tokio::test]
    async fn an_append_over_the_bound_is_not_kept() {
        let published = PublishedAppend::with_max_bytes(1);
        let _: Vec<RecordBatch> = published
            .keep(stream(vec![batch(vec![1, 2, 3])]))
            .try_collect()
            .await
            .expect("stream");
        assert!(matches!(published.take(), KeptAppend::Overflowed));
    }

    #[tokio::test]
    async fn a_committed_append_reaches_subscribers() {
        let broadcaster = DataUpdateBroadcaster::new();
        let table = TableReference::bare("events");
        let mut receiver = broadcaster.subscribe(&table).await;
        let publisher = DataUpdatePublisher::new(broadcaster, table.clone());

        let guard = publisher.begin_commit().await;
        publish_committed_append(
            &publisher,
            guard,
            &table,
            schema(),
            KeptAppend::Rows(vec![batch(vec![7, 8])]),
        )
        .await;

        let update = receiver.recv().await.expect("an update");
        assert!(matches!(update.update_type, UpdateType::Append));
        assert_eq!(
            update.data.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2
        );
    }

    #[tokio::test]
    async fn an_overflowed_append_ends_subscriptions() {
        let broadcaster = DataUpdateBroadcaster::new();
        let table = TableReference::bare("events");
        let mut receiver = broadcaster.subscribe(&table).await;
        let publisher = DataUpdatePublisher::new(broadcaster, table.clone());

        let guard = publisher.begin_commit().await;
        publish_committed_append(&publisher, guard, &table, schema(), KeptAppend::Overflowed).await;

        assert!(matches!(
            receiver.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Closed)
        ));
    }
}
