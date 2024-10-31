/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::TableProvider, error::DataFusionError, execution::RecordBatchStream,
    physical_plan::collect, prelude::SessionContext,
};
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast, Barrier, BarrierWaitResult};
use tokio::task::JoinSet;
use tokio_stream::wrappers::BroadcastStream;
use util::RetryError;

use crate::{
    accelerated_table::{refresh_task::retry_from_df_error, synchronized_table::SynchronizedTable},
    dataupdate::StreamingDataUpdateExecutionPlan,
};

pub(crate) struct MultiSink {
    original_table_provider: Arc<dyn TableProvider>,
    synchronized_tables: Vec<SynchronizedTable>,
}

impl MultiSink {
    pub fn new(
        original_table_provider: Arc<dyn TableProvider>,
        synchronized_tables: Vec<SynchronizedTable>,
    ) -> Self {
        Self {
            original_table_provider,
            synchronized_tables,
        }
    }

    pub fn add_synchronized_table(&mut self, synchronized_table: SynchronizedTable) {
        self.synchronized_tables.push(synchronized_table);
    }

    pub fn synchronized_tables(&self) -> &Vec<SynchronizedTable> {
        &self.synchronized_tables
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: bool,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let schema = record_batch_stream.schema();
        let (tx, _) = broadcast::channel::<RecordBatch>(32);
        let mut join_set = JoinSet::new();

        let ctx = SessionContext::new();

        let total_streams = self.table_providers().len();
        let barrier = Arc::new(Barrier::new(total_streams));

        // Spawn tasks for each table provider
        for provider in self.table_providers() {
            let rx = tx.subscribe();
            let ctx_state = ctx.state();
            let schema = Arc::clone(&schema);
            let barrier = Arc::clone(&barrier);

            join_set.spawn(async move {
                let stream = RecordBatchBroadcastStream::new(rx, schema, barrier);

                let insertion_plan = provider
                    .insert_into(
                        &ctx_state,
                        Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(stream))),
                        overwrite,
                    )
                    .await
                    .map_err(|e| {
                        RetryError::permanent(crate::accelerated_table::Error::FailedToWriteData {
                            source: e,
                        })
                    })?;

                collect(insertion_plan, ctx_state.task_ctx())
                    .await
                    .map_err(retry_from_df_error)?;

                Ok(())
            });
        }

        // Process the input stream and broadcast to all receivers
        let mut stream = record_batch_stream;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(retry_from_df_error)?;

            if tx.send(batch).is_err() {
                break;
            }
        }

        // Drop the sender to signal completion to all receivers
        drop(tx);

        // Wait for all tasks to complete and collect any errors
        let mut errors = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => (),
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(RetryError::Permanent(
                    crate::accelerated_table::Error::FailedToWriteData {
                        source: DataFusionError::External(Box::new(e)),
                    },
                )),
            }
        }

        // If any tasks failed, return the first error
        if let Some(first_error) = errors.into_iter().next() {
            return Err(first_error);
        }

        Ok(())
    }

    fn table_providers(&self) -> Vec<Arc<dyn TableProvider>> {
        let mut table_providers = vec![Arc::clone(&self.original_table_provider)];
        table_providers.extend(
            self.synchronized_tables
                .iter()
                .map(SynchronizedTable::child_accelerator),
        );
        table_providers
    }
}

// Stream implementation that wraps a broadcast::Receiver
struct RecordBatchBroadcastStream {
    inner: BroadcastStream<RecordBatch>,
    schema: SchemaRef,
    barrier: Arc<Barrier>,
    reached_end: bool,
    barrier_wait: Option<Pin<Box<dyn Future<Output = BarrierWaitResult> + Send>>>,
}

impl RecordBatchBroadcastStream {
    fn new(rx: broadcast::Receiver<RecordBatch>, schema: SchemaRef, barrier: Arc<Barrier>) -> Self {
        Self {
            inner: BroadcastStream::new(rx),
            schema,
            barrier,
            reached_end: false,
            barrier_wait: None,
        }
    }
}

impl Stream for RecordBatchBroadcastStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.reached_end {
            // If we've reached the end, poll the barrier wait future
            if let Some(wait) = self.barrier_wait.as_mut() {
                match wait.as_mut().poll(cx) {
                    Poll::Ready(_) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(None) => {
                self.reached_end = true;
                // Create the barrier wait future when we first reach the end
                let barrier = Arc::clone(&self.barrier);
                self.barrier_wait = Some(Box::pin(async move { barrier.wait().await }));
                // Poll it immediately
                if let Some(wait) = self.barrier_wait.as_mut() {
                    match wait.as_mut().poll(cx) {
                        Poll::Ready(_) => Poll::Ready(None),
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    unreachable!()
                }
            }
            other => other
                .map(|opt| opt.map(|res| res.map_err(|e| DataFusionError::External(Box::new(e))))),
        }
    }
}

impl RecordBatchStream for RecordBatchBroadcastStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use std::time::Duration;
    use tokio::time::sleep;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn create_test_record_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            create_test_schema(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("valid record batch")
    }

    #[tokio::test]
    async fn test_data_transmission() {
        let barrier = Arc::new(Barrier::new(1));
        let (tx, rx) = broadcast::channel(32);
        let schema = create_test_schema();

        let mut stream = RecordBatchBroadcastStream::new(rx, schema, barrier);

        // Send multiple batches with actual data
        let batches = vec![
            create_test_record_batch(&[1, 2, 3]),
            create_test_record_batch(&[4, 5, 6]),
        ];

        for batch in &batches {
            tx.send(batch.clone()).expect("to send record batch");
        }
        drop(tx);

        let mut received_values: Vec<i32> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.expect("valid record batch");
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("valid int32 array");
            received_values.extend(array.values());
        }

        assert_eq!(received_values, vec![1, 2, 3, 4, 5, 6]);
    }

    #[allow(clippy::cast_possible_wrap)]
    #[tokio::test]
    async fn test_barrier_synchronization() {
        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx1) = broadcast::channel(32);
        let rx2 = tx.subscribe();
        let schema = create_test_schema();

        // Create streams with different processing delays
        let mut stream1 =
            RecordBatchBroadcastStream::new(rx1, Arc::clone(&schema), Arc::clone(&barrier));
        let mut stream2 = RecordBatchBroadcastStream::new(rx2, schema, barrier);

        let now = std::time::Instant::now();

        // Task 1: Process quickly
        let task1 = tokio::spawn(async move {
            let mut results = Vec::new();
            while let Some(result) = stream1.next().await {
                results.push(result.expect("valid record batch"));
            }
            (results, now.elapsed())
        });

        // Task 2: Add artificial delay
        let task2 = tokio::spawn(async move {
            let mut results = Vec::new();
            while let Some(result) = stream2.next().await {
                sleep(Duration::from_millis(100)).await;
                results.push(result.expect("valid record batch"));
            }
            (results, now.elapsed())
        });

        // Send test data
        tx.send(create_test_record_batch(&[1, 2, 3]))
            .expect("to send record batch");
        drop(tx);

        // Wait for both tasks and handle their results
        let (task1_result, task2_result) = tokio::join!(task1, task2);
        let (results1, time1) = task1_result.expect("task 1 completed");
        let (results2, time2) = task2_result.expect("task 2 completed");

        // Verify both streams received the same data
        assert_eq!(results1.len(), 1);
        assert_eq!(results2.len(), 1);

        // Verify barrier caused both streams to complete at approximately the same time
        assert!(time1.as_millis() >= 100); // Fast stream should wait for slow stream
        assert!((time1.as_millis() as i128 - time2.as_millis() as i128).abs() < 50);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let barrier = Arc::new(Barrier::new(1));
        let (tx, rx) = broadcast::channel::<RecordBatch>(1); // Small buffer to force lagging error
        let schema = create_test_schema();

        let mut stream = RecordBatchBroadcastStream::new(rx, schema, barrier);

        // Fill the channel and cause a lagging error
        for i in 0..10 {
            tx.send(create_test_record_batch(&[i])).unwrap_or_default();
        }

        // Verify we get an error when reading from a lagged stream
        if let Some(result) = stream.next().await {
            assert!(result.is_err());
        } else {
            panic!("expected error");
        }
    }
}
